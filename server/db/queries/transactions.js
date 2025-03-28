/**
 * @file Defines the queries for the transactions table.
 */

const { retrieveAccountByPlaidAccountId } = require('./accounts');
const db = require('../');

/**
 * Creates or updates multiple transactions.
 *
 * @param {Object[]} transactions an array of transactions.
 */
console.log('🔄 update_transactions.js script started!');


const createOrUpdateTransactions = async transactions => {
  const pendingQueries = transactions.map(async transaction => {
    const {
      account_id: plaidAccountId,
      transaction_id: plaidTransactionId,
      personal_finance_category: { primary: category },
      transaction_type: transactionType,
      name: transactionName,
      amount,
      iso_currency_code: isoCurrencyCode,
      unofficial_currency_code: unofficialCurrencyCode,
      date: transactionDate,
      pending,
      account_owner: accountOwner,
    } = transaction;

    const { id: accountId } = await retrieveAccountByPlaidAccountId(plaidAccountId);

    try {
      // Simplified insert with ON CONFLICT to handle duplicates
      const upsertQuery = {
        text: `
          INSERT INTO transactions_table
            (account_id, plaid_transaction_id, category, type, name, amount,
             iso_currency_code, unofficial_currency_code, date, pending, account_owner, potential_duplicate)
          VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, FALSE)
          ON CONFLICT (plaid_transaction_id)
          DO UPDATE SET
            account_id = EXCLUDED.account_id,
            category = EXCLUDED.category,
            type = EXCLUDED.type,
            name = EXCLUDED.name,
            amount = EXCLUDED.amount,
            iso_currency_code = EXCLUDED.iso_currency_code,
            unofficial_currency_code = EXCLUDED.unofficial_currency_code,
            date = EXCLUDED.date,
            pending = EXCLUDED.pending,
            account_owner = EXCLUDED.account_owner,
            potential_duplicate = EXCLUDED.potential_duplicate;
        `,
        values: [
          accountId,
          plaidTransactionId,
          category,
          transactionType,
          transactionName,
          amount,
          isoCurrencyCode,
          unofficialCurrencyCode,
          transactionDate,
          pending,
          accountOwner
        ],
      };

      await db.query(upsertQuery);
      console.log(`Upserted transaction: ${transactionName} (${plaidTransactionId}).`);

    } catch (err) {
      console.error(`Error processing transaction ${transactionName}:`, err);
    }
  });

  await Promise.all(pendingQueries);
};



/**
 * Retrieves all transactions for a single account.
 *
 * @param {number} accountId the ID of the account.
 * @returns {Object[]} an array of transactions.
 */
const retrieveTransactionsByAccountId = async accountId => {
  const query = {
    text: 'SELECT * FROM transactions WHERE account_id = $1 ORDER BY date DESC',
    values: [accountId],
  };
  const { rows: transactions } = await db.query(query);
  return transactions;
};

/**
 * Retrieves all transactions for a single item.
 *
 *
 * @param {number} itemId the ID of the item.
 * @returns {Object[]} an array of transactions.
 */
const retrieveTransactionsByItemId = async itemId => {
  const query = {
    text: 'SELECT * FROM transactions WHERE item_id = $1 ORDER BY date DESC',
    values: [itemId],
  };
  const { rows: transactions } = await db.query(query);
  return transactions;
};

/**
 * Retrieves all transactions for a single user.
 *
 *
 * @param {number} userId the ID of the user.
 * @returns {Object[]} an array of transactions.
 */
const retrieveTransactionsByUserId = async userId => {
  const query = {
    text: 'SELECT * FROM transactions WHERE user_id = $1 ORDER BY date DESC',
    values: [userId],
  };
  const { rows: transactions } = await db.query(query);
  return transactions;
};

/**
 * Removes one or more transactions.
 *
 * @param {string[]} plaidTransactionIds the Plaid IDs of the transactions.
 */
const deleteTransactions = async plaidTransactionIds => {
  const pendingQueries = plaidTransactionIds.map(async transactionId => {
    const query = {
      text: 'DELETE FROM transactions_table WHERE plaid_transaction_id = $1',
      values: [transactionId],
    };
    await db.query(query);
  });
  await Promise.all(pendingQueries);
};

module.exports = {
  createOrUpdateTransactions,
  retrieveTransactionsByAccountId,
  retrieveTransactionsByItemId,
  retrieveTransactionsByUserId,
  deleteTransactions,
};
