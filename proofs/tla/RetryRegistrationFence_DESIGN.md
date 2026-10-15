# Retry registration fence

Marker tokens are allocated before a retried registration transaction. Allocation order therefore need not match commit order. The model separates these actions and permits forget/re-add between them.

A registration must observe both the captured peer incarnation and a marker no newer than its allocated token. The positive configuration checks both conditions; the two negative configurations remove one condition each and violate `INV_RegistrationCannotCrossFence`.

The bounds are two tokens and two peer incarnations. Transport, payload discovery, retry scheduling, and the Rust implementation are outside this boundary model. Already-admitted external I/O is modeled separately from authorization of a future push.
