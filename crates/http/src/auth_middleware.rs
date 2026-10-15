//! Global auth middleware for consistent access control enforcement.
//!
//! Intercepts every matched request, parses the identity from the
//! Authorization header, and enforces the route's permission requirement
//! before the handler runs. Existing per-handler `require_permission()` calls
//! remain as a redundant safety net.
//!
//! It also binds the parsed identity to the request task (via the
//! `current_identity` task-local) so DB-layer NAC checks on REST paths can
//! resolve who is acting — reusing the single parse done here rather than a
//! separate middleware that re-verified the JWT.

use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::identity_extractor::{parse_identity_from_headers, ExtractIdentity, MiddlewareIdentity};
use crate::nac_guard::require_permission;
use crate::route_permissions::{route_permission, RoutePermission};
use crate::router::AppState;

/// Auth middleware that enforces route-level permissions.
///
/// Applied via `Router::route_layer()` so it runs after routing
/// (MatchedPath is available) but before the handler.
pub async fn auth_middleware(
    State(state): State<AppState>,
    matched_path: Option<MatchedPath>,
    mut request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = matched_path
        .as_ref()
        .map(|mp| mp.as_str())
        .unwrap_or_else(|| {
            tracing::warn!("MatchedPath unavailable in auth middleware — allowing request");
            ""
        });

    let permission = route_permission(path, &method);
    let is_exempt = matches!(permission, RoutePermission::Exempt);

    // Parse the caller's identity once. The same value is reused below both to
    // pre-populate the handler extension and to bind the request-scoped NAC
    // context, so the JWT is verified only once per request.
    let acting_did = match permission {
        RoutePermission::Exempt => None,
        RoutePermission::IdentityOnly | RoutePermission::Dynamic | RoutePermission::Required(_) => {
            match parse_identity_from_headers(request.headers()) {
                Ok(did) => did,
                Err(err) => return err.into_response(),
            }
        }
    };

    if !is_exempt {
        request
            .extensions_mut()
            .insert(MiddlewareIdentity(acting_did.clone()));
    }

    // Bind the caller's DID to the request task so DB-layer NAC checks on REST
    // paths resolve who is acting (survives `.await`, mirroring Go's
    // `identity.FromContext`). This replaces a separate outer middleware that
    // re-parsed the JWT.
    let scoped = acting_did.as_ref().map(ToString::to_string);
    // Capture this request's verified token directly from its header. Never
    // publish it in the process-wide DID map: concurrent requests can share a
    // DID while carrying different authorization tokens.
    let bearer = acting_did.as_ref().and_then(|_| {
        request
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                value
                    .strip_prefix("Bearer ")
                    .or_else(|| value.strip_prefix("bearer "))
            })
            .map(|token| token.trim().to_owned())
    });
    let execution = async move {
        if let RoutePermission::Required(perm) = permission {
            let identity = ExtractIdentity::from_did(acting_did);
            if let Err(err) = require_permission(&state, &identity, perm).await {
                return err.into_response();
            }
        }
        next.run(request).await
    };
    defra_core::current_identity::with_scoped_identity(
        scoped.clone(),
        defra_core::signing::scope_request_bearer_token(scoped, bearer, execution),
    )
    .await
}
