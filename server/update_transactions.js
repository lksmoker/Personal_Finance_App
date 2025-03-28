/**
 * @file Defines helpers for updating transactions on an item
 */

console.log('🚀 Script is running, but before any function is called!');

const plaid = require('./plaid');
const {
  retrieveItemByPlaidItemId,
  createAccounts,
  createOrUpdateTransactions,
  deleteTransactions,
  updateItemTransactionsCursor,
  retrieveItemById, // Moved this up to combine imports
} = require('./db/queries');
const db = require('./db');

/**
 * Fetches transactions from the Plaid API for a given item.
 *
 * @param {string} plaidItemId the Plaid ID for the item.
 * @returns {Object{}} an object containing transactions and a cursor.
 */
const fetchTransactionUpdates = async (plaidItemId) => {
  const item = await retrieveItemByPlaidItemId(plaidItemId);
  console.log('🔍 Retrieved item:', item);  // Add this debug log

  if (!item) {
    throw new Error(`Item with Plaid Item ID ${plaidItemId} not found in the database.`);
  }

  const { plaid_access_token: accessToken, transactions_cursor: lastCursor } = item;

  let cursor = lastCursor;

  let added = [];
  let modified = [];
  let removed = [];
  let hasMore = true;

  const batchSize = 100;
  try {
    while (hasMore) {
      const request = { access_token: accessToken, cursor, count: batchSize };
      const response = await plaid.transactionsSync({
        access_token: accessToken,
        cursor: cursor || null,  // Use saved cursor or null if no cursor exists
      });
      
      const data = response.data;

      added = added.concat(data.added);
      modified = modified.concat(data.modified);
      removed = removed.concat(data.removed);
      hasMore = data.has_more;
      cursor = data.next_cursor;
    }
  } catch (err) {
    console.error(`Error fetching transactions: ${err.message}`);
    cursor = lastCursor;
  }
  return { added, modified, removed, cursor, accessToken };
};

/**
 * Handles the fetching and storing of new, modified, or removed transactions
 *
 * @param {string} plaidItemId the Plaid ID for the item.
 */
const updateTransactions = async (plaidItemId) => {
  const { added, modified, removed, cursor, accessToken } = await fetchTransactionUpdates(plaidItemId);

  const { data: { accounts } } = await plaid.accountsGet({ access_token: accessToken });

  await createAccounts(plaidItemId, accounts);
  await createOrUpdateTransactions(added.concat(modified));
console.log("✅ Processed transactions:", added.concat(modified).map(t => t.name));

  await deleteTransactions(removed);
  await updateItemTransactionsCursor(plaidItemId, cursor);

  return {
    addedCount: added.length,
    modifiedCount: modified.length,
    removedCount: removed.length,
  };
};

// Combined the two async functions into one
(async () => {
  console.log('🔄 update_transactions.js script started!');

  try {
    // Fetch all items from the database
    const items = await retrieveItemById();

    if (!items || items.length === 0) {
      console.log('No items found to sync transactions.');
      return;
    }

    // Loop through each item and update transactions
    for (const item of items) {
      console.log(`🔄 Syncing transactions for Plaid Item ID: ${item.plaid_item_id}`);
      await updateTransactions(item.plaid_item_id);  // Passing to the function as plaidItemId
    }

    // Test database connection after syncing transactions
    const result = await db.query('SELECT * FROM items');
    console.log(`📦 Retrieved ${result.rows.length} items from the database`);

    result.rows.forEach(item => {
      console.log(`Item ID: ${item.id}, Access Token: ${item.plaid_access_token}`);
    });

  } catch (error) {
    console.error('❌ Error during transaction sync or database connection:', error);
  }
})();

// Export the function