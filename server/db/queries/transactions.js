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
      // Check for similar transactions (potential duplicates)
      const existingTransactionQuery = {
        text: `
          SELECT * FROM transactions_table
          WHERE account_id = $1
            AND ABS(amount - $2) <= 1.00
            AND date = $3
        `,
        values: [accountId, amount, transactionDate],
      };

      const { rows: existingTransactions } = await db.query(existingTransactionQuery);

      if (existingTransactions.length > 0) {
        const existingTransaction = existingTransactions[0];

        // 🔍 Debug log to confirm duplicate detection
        console.log(`Potential duplicate detected for ${transactionName} on ${transactionDate}`);

        if (existingTransaction.pending && !pending) {
          // Update the pending transaction to the posted version
          const updateQuery = {
            text: `
              UPDATE transactions_table
              SET plaid_transaction_id = $1,
                  category = $2,
                  type = $3,
                  name = $4,
                  amount = $5,
                  iso_currency_code = $6,
                  unofficial_currency_code = $7,
                  pending = FALSE,
                  account_owner = $8,
                  potential_duplicate = FALSE
              WHERE id = $9
            `,
            values: [
              plaidTransactionId,
              category,
              transactionType,
              transactionName,
              amount,
              isoCurrencyCode,
              unofficialCurrencyCode,
              accountOwner,
              existingTransaction.id
            ],
          };
          await db.query(updateQuery);
          console.log(`Updated pending transaction for ${transactionName} to posted.`);
        } else {
          // Flag as potential duplicate
          const flagDuplicateQuery = {
            text: `
              INSERT INTO transactions_table
                (account_id, plaid_transaction_id, category, type, name, amount,
                 iso_currency_code, unofficial_currency_code, date, pending, account_owner, potential_duplicate)
              VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, TRUE)
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
          await db.query(flagDuplicateQuery);
          console.log(`Flagged potential duplicate transaction: ${transactionName}.`);
        }
      } else {
        // No duplicate found, insert as new
        const insertQuery = {
          text: `
            INSERT INTO transactions_table
              (account_id, plaid_transaction_id, category, type, name, amount,
               iso_currency_code, unofficial_currency_code, date, pending, account_owner, potential_duplicate)
            VALUES
              ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, FALSE)
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
        await db.query(insertQuery);
        console.log(`Inserted new transaction: ${transactionName}.`);
      }
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
