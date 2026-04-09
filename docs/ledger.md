# Ledger and Allocations


My current path of solving the ledger is to create a core object `Allocation` that is stored on a separate table maybe even table per tradespace (due to performance impact) that will enable allocating funds to be used by an agent. 
For example, we might want to allocate some of our money to a `PolymarketOrder`, but we can't just simply create the order and then subtract the balance out of the object, as it could get us into the negative easily.
When a user creates an `Allocation` the system should lock `Allection` creation for the `Tradespace`. Sum up all active allocations (could be positive and negative) and append an `Allocation` if `amount > available`.
