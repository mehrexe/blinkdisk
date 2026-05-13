import { Pool } from "pg";
import { generateId } from "../../utils/src/id";

const TRIAL_CAPACITY = 15 * 1000 * 1000 * 1000;
const BATCH_SIZE = 500;

type AccountTrialRow = {
  accountId: string;
  accountCreatedAt: Date;
  trialId: string | null;
};

type TrialRow = {
  id: string;
};

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required");
}

const pool = new Pool({
  connectionString: databaseUrl,
  max: 1,
});

async function fetchBatch(afterAccountId: string | null) {
  const result = await pool.query<AccountTrialRow>(
    `
      SELECT
        account."id" AS "accountId",
        account."createdAt" AS "accountCreatedAt",
        trial."id" AS "trialId"
      FROM "Account" account
      LEFT JOIN "Trial" trial ON trial."accountId" = account."id"
      WHERE ($1::text IS NULL OR account."id" > $1)
      ORDER BY account."id"
      LIMIT $2
    `,
    [afterAccountId, BATCH_SIZE],
  );

  return result.rows;
}

async function backfillBatch(rows: AccountTrialRow[]) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const row of rows) {
      const trialId = row.trialId ?? generateId("Trial");

      const trial = await client.query<TrialRow>(
        `
          INSERT INTO "Trial" (
            "id",
            "status",
            "capacity",
            "accountId",
            "startedAt"
          )
          VALUES ($1, 'ACTIVE', $2, $3, $4)
          ON CONFLICT ("accountId") DO UPDATE
          SET
            "status" = 'ACTIVE',
            "capacity" = EXCLUDED."capacity",
            "startedAt" = EXCLUDED."startedAt"
          RETURNING "id"
        `,
        [
          trialId,
          TRIAL_CAPACITY.toString(),
          row.accountId,
          row.accountCreatedAt,
        ],
      );

      const linkedTrialId = trial.rows[0]?.id;

      if (!linkedTrialId) {
        throw new Error(`Failed to upsert trial for account ${row.accountId}`);
      }

      await client.query(
        `
          UPDATE "Space"
          SET "trialId" = $1
          WHERE "accountId" = $2
        `,
        [linkedTrialId, row.accountId],
      );
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function main() {
  let afterAccountId: string | null = null;
  let processed = 0;

  while (true) {
    const rows = await fetchBatch(afterAccountId);

    if (rows.length === 0) break;

    await backfillBatch(rows);

    processed += rows.length;
    afterAccountId = rows.at(-1)?.accountId ?? null;

    console.info(`Backfilled trial data for ${processed} accounts`);
  }
}

main()
  .catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
