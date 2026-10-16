use std::cell::{BorrowError, BorrowMutError, Ref, RefCell, RefMut};
use std::error::Error;

use lens_sdk::error::LensError;
use lens_sdk::StreamOption;
use serde::Deserialize;

lens_sdk::define!(PARAMETERS: Parameters, try_transform, try_inverse);

#[derive(Deserialize, Clone)]
pub struct Parameters {
    pub dst: String,
    pub value: serde_json::Value,
}

type Row = serde_json::Map<String, serde_json::Value>;

/// The parameter slot the `lens_sdk` macros expect: `read()` and `write()` returning a `Result`.
struct ParameterSlot(RefCell<Option<Parameters>>);

// SAFETY: a wasm lens guest runs on a single thread, so the slot is never shared across threads.
unsafe impl Sync for ParameterSlot {}

impl ParameterSlot {
    fn read(&self) -> Result<Ref<'_, Option<Parameters>>, BorrowError> {
        self.0.try_borrow()
    }

    fn write(&self) -> Result<RefMut<'_, Option<Parameters>>, BorrowMutError> {
        self.0.try_borrow_mut()
    }
}

static PARAMETERS: ParameterSlot = ParameterSlot(RefCell::new(None));

fn try_transform(
    iter: &mut dyn Iterator<Item = lens_sdk::Result<Option<Row>>>,
) -> Result<StreamOption<Row>, Box<dyn Error>> {
    let params = PARAMETERS
        .read()?
        .clone()
        .ok_or(LensError::ParametersNotSetError)?;

    for item in iter {
        let mut input = match item? {
            Some(v) => v,
            None => return Ok(StreamOption::None),
        };

        input.insert(params.dst, params.value.clone());

        return Ok(StreamOption::Some(input));
    }

    Ok(StreamOption::EndOfStream)
}

fn try_inverse(
    iter: &mut dyn Iterator<Item = lens_sdk::Result<Option<Row>>>,
) -> Result<StreamOption<Row>, Box<dyn Error>> {
    let params = PARAMETERS
        .read()?
        .clone()
        .ok_or(LensError::ParametersNotSetError)?;

    for item in iter {
        let mut input = match item? {
            Some(v) => v,
            None => return Ok(StreamOption::None),
        };

        input.remove(&params.dst);

        return Ok(StreamOption::Some(input));
    }

    Ok(StreamOption::EndOfStream)
}
