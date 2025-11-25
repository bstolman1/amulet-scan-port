# Welcome to your Lovable project

## Project info

**URL**: https://lovable.dev/projects/fdd2ffc6-5832-4389-80f4-e69bba15aa8b

## How can I edit this code?

There are several ways of editing your application.

**Use Lovable**

Simply visit the [Lovable Project](https://lovable.dev/projects/fdd2ffc6-5832-4389-80f4-e69bba15aa8b) and start prompting.

Changes made via Lovable will be committed automatically to this repo.

**Use your preferred IDE**

If you want to work locally using your own IDE, you can clone this repo and push changes. Pushed changes will also be reflected in Lovable.

The only requirement is having Node.js & npm installed - [install with nvm](https://github.com/nvm-sh/nvm#installing-and-updating)

Follow these steps:

```sh
# Step 1: Clone the repository using the project's Git URL.
git clone <YOUR_GIT_URL>

# Step 2: Navigate to the project directory.
cd <YOUR_PROJECT_NAME>

# Step 3: Install the necessary dependencies.
npm i

# Step 4: Start the development server with auto-reloading and an instant preview.
npm run dev
```

**Edit a file directly in GitHub**

- Navigate to the desired file(s).
- Click the "Edit" button (pencil icon) at the top right of the file view.
- Make your changes and commit the changes.

**Use GitHub Codespaces**

- Navigate to the main page of your repository.
- Click on the "Code" button (green button) near the top right.
- Select the "Codespaces" tab.
- Click on "New codespace" to launch a new Codespace environment.
- Edit files directly within the Codespace and commit and push your changes once you're done.

## What technologies are used for this project?

This project is built with:

- Vite
- TypeScript
- React
- shadcn-ui
- Tailwind CSS

## How can I deploy this project?

Simply open [Lovable](https://lovable.dev/projects/fdd2ffc6-5832-4389-80f4-e69bba15aa8b) and click on Share -> Publish.

## Can I connect a custom domain to my Lovable project?

Yes, you can!

To connect a domain, navigate to Project > Settings > Domains and click Connect Domain.

Read more here: [Setting up a custom domain](https://docs.lovable.dev/features/custom-domain#custom-domain)

## Backfill Performance Optimizations

The backfill script includes optional performance optimizations that can provide **3×–15× speed improvements** for bulk data ingestion:

### Enabling Optimizations

Set the `ENABLE_OPTIMIZATIONS=true` environment variable when running the backfill:

```sh
ENABLE_OPTIMIZATIONS=true node scripts/fetch-backfill-history.js
```

### What Gets Optimized

1. **UNLOGGED Tables** (3×–10× faster)
   - Tables are made UNLOGGED during backfill
   - Automatically restored to LOGGED after completion

2. **Index Dropping** (5×–15× faster)
   - All indexes are dropped before ingestion
   - Automatically recreated after completion

3. **Large Batch Sizes** (2×–4× faster)
   - Batch size increased to 10,000 rows
   - Reduces COPY operation overhead

### Important Notes

- ⚠️ **UNLOGGED tables are not crash-safe**: Data may be lost if the database crashes during backfill
- ✅ **Automatic restoration**: The script automatically restores normal settings after completion or on error
- 🔒 **Use for initial backfill only**: Not recommended for production live ingestion

### Manual Control (Advanced)

If you need manual control over optimizations:

**Before backfill:**
```sql
ALTER TABLE ledger_updates SET UNLOGGED;
ALTER TABLE ledger_events SET UNLOGGED;
DROP INDEX IF EXISTS idx_ledger_updates_migration_id;
DROP INDEX IF EXISTS idx_ledger_events_migration_id;
```

**After backfill:**
```sql
ALTER TABLE ledger_updates SET LOGGED;
ALTER TABLE ledger_events SET LOGGED;
CREATE INDEX idx_ledger_updates_migration_id ON ledger_updates(migration_id);
CREATE INDEX idx_ledger_events_migration_id ON ledger_events(migration_id);
```
