import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Download, FileText, Copy, Check } from "lucide-react";
import { useState } from "react";
import { useToast } from "@/hooks/use-toast";

const DOCUMENTATION_CONTENT = `# Canton Network Template Documentation

**Generated:** ${new Date().toISOString().split('T')[0]}

This document provides comprehensive documentation for all Canton Network templates, explaining their JSON structure, purpose, and use cases.

---

## Core Currency Templates

### Splice.Amulet:Amulet

**Purpose:** Represents the primary Amulet token in the Canton Network

**Description:**
Amulet is the native digital currency of the Canton Network. Each Amulet contract represents a specific amount of tokens held by an owner. Amulets can be transferred, locked, and used for various network operations including paying transaction fees and participating in governance.

**Use Cases:**
- Storing value on the network
- Paying transaction fees
- Rewarding validators and super validators
- Participating in governance decisions

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "owner": "<party_id>",
  "amount": {
    "initialAmount": "1000.0000000000",
    "createdAt": { "number": "79500" },
    "ratePerRound": { "rate": "0.0000001903" }
  }
}
\`\`\`

**Field Descriptions:**
| Field | Description |
|-------|-------------|
| dso | The DSO (Decentralized Synchronizer Operator) party that manages the network |
| owner | The party that owns this Amulet |
| amount.initialAmount | The initial amount of Amulet tokens (with high precision) |
| amount.createdAt.number | Round number when this Amulet was created |
| amount.ratePerRound.rate | The holding fee rate applied per round |

---

### Splice.Amulet:LockedAmulet

**Purpose:** Represents Amulet tokens that are locked for a specific purpose

**Description:**
LockedAmulet contracts represent Amulet tokens that have been locked, typically for time-based vesting, collateral, or other purposes requiring temporary immobilization of funds.

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "amulet": {
    "owner": "<party_id>",
    "amount": {
      "initialAmount": "500.0000000000",
      "createdAt": { "number": "79000" },
      "ratePerRound": { "rate": "0.0000001903" }
    }
  },
  "lock": {
    "holders": ["<party_id>"],
    "expiresAt": { "number": "80000" }
  }
}
\`\`\`

---

## Validator Operations Templates

### Splice.ValidatorLicense:ValidatorLicense

**Purpose:** Authorizes a party to operate as a validator

**Description:**
ValidatorLicense contracts grant parties the authority to operate as validators on the Canton Network. Validators are responsible for processing transactions and maintaining network consensus.

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "validator": "<party_id>",
  "sponsor": "<party_id>",
  "faucetState": {
    "tag": "FaucetState",
    "value": {
      "numCouponsMissed": "0",
      "firstReceivedFor": { "number": "1000" },
      "lastReceivedFor": { "number": "79500" }
    }
  }
}
\`\`\`

---

### Splice.Amulet:ValidatorRight

**Purpose:** Grants rights to operate as a validator for a specific user

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "user": "<party_id>",
  "validator": "<party_id>"
}
\`\`\`

---

## Rewards Templates

### Splice.Amulet:AppRewardCoupon

**Purpose:** Represents unclaimed rewards earned by application providers

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "provider": "<party_id>",
  "round": { "number": "79500" },
  "amount": "10.5000000000",
  "featured": true
}
\`\`\`

---

### Splice.Amulet:SvRewardCoupon

**Purpose:** Represents unclaimed rewards for Super Validators

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "sv": "<party_id>",
  "round": { "number": "79500" },
  "weight": "10000"
}
\`\`\`

---

### Splice.Amulet:ValidatorRewardCoupon

**Purpose:** Represents unclaimed rewards for validators

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "user": "<party_id>",
  "validator": "<party_id>",
  "round": { "number": "79500" }
}
\`\`\`

---

## Governance Templates

### Splice.DsoRules:DsoRules

**Purpose:** Defines the core rules for DSO operation

**Description:**
DsoRules is the central governance contract that defines how the Decentralized Synchronizer Operator functions. It contains network parameters, fee schedules, SV weights, and governance rules.

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "epoch": "42",
  "svs": {
    "<sv_party_id>": {
      "name": "SV-1",
      "weight": "10000",
      "joinedAt": "2025-01-01T00:00:00Z"
    }
  },
  "config": {
    "decentralizedSynchronizer": {
      "synchronizers": ["<sync_id>"],
      "requiredSynchronizers": 1
    },
    "actionConfirmationTimeout": "PT1H"
  }
}
\`\`\`

---

### Splice.DsoRules:VoteRequest

**Purpose:** Represents a governance proposal awaiting votes

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "requester": "<party_id>",
  "action": {
    "tag": "ARC_DsoRules",
    "value": {
      "tag": "SRARC_OffboardSv",
      "value": { "sv": "<sv_party_id>" }
    }
  },
  "reason": {
    "url": "https://governance.example.com/proposal/123",
    "body": "Proposal description"
  },
  "votes": {},
  "expiresAt": "2026-01-28T00:00:00Z"
}
\`\`\`

---

## Mining Rounds Templates

### Splice.Round:OpenMiningRound

**Purpose:** Represents a currently open mining round

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "round": { "number": "79500" },
  "amuletPrice": "0.005",
  "opensAt": "2026-01-21T20:00:00Z",
  "targetClosesAt": "2026-01-21T20:10:00Z"
}
\`\`\`

---

### Splice.Round:ClosedMiningRound

**Purpose:** Represents a completed mining round

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "round": { "number": "79497" },
  "amuletPrice": "0.005"
}
\`\`\`

---

### Splice.Round:IssuingMiningRound

**Purpose:** Represents a round currently issuing rewards

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "round": { "number": "79495" },
  "issuanceConfig": {
    "amuletToIssuePerYear": "40000000.0",
    "validatorRewardPercentage": "0.05",
    "appRewardPercentage": "0.15"
  }
}
\`\`\`

---

## ANS (Naming Service) Templates

### Splice.Ans:AnsEntry

**Purpose:** Represents a registered name in the ANS

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "user": "<party_id>",
  "name": "alice.canton",
  "url": "https://alice.example.com",
  "description": "Alice's Canton identity",
  "expiresAt": "2027-01-21T00:00:00Z"
}
\`\`\`

---

## Traffic Templates

### Splice.DecentralizedSynchronizer:MemberTraffic

**Purpose:** Tracks member traffic on the synchronizer

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "member": "<party_id>",
  "synchronizer": "<synchronizer_id>",
  "totalTrafficPurchased": "1000000000",
  "totalTrafficConsumed": "500000000"
}
\`\`\`

---

## Transfer Templates

### Splice.AmuletRules:TransferPreapproval

**Purpose:** Pre-authorizes a future transfer

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "sender": "<party_id>",
  "receiver": "<party_id>",
  "provider": "<party_id>"
}
\`\`\`

---

### Splice.ExternalPartyAmuletRules:TransferCommand

**Purpose:** Commands a transfer from an external party

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "sender": "<party_id>",
  "receiver": "<party_id>",
  "amount": "100.0000000000"
}
\`\`\`

---

## External Party Templates

### Splice.ExternalPartyAmuletRules:ExternalPartyAmuletRules

**Purpose:** Defines rules for external party operations

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "party": "<party_id>"
}
\`\`\`

---

## Subscription Templates

### Splice.Wallet.Subscriptions:Subscription

**Purpose:** Represents an active subscription

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "subscriber": "<party_id>",
  "provider": "<party_id>",
  "subscriptionData": {
    "paymentAmount": "10.0",
    "paymentInterval": "P30D"
  }
}
\`\`\`

---

## Applications Templates

### Splice.Amulet:FeaturedAppRight

**Purpose:** Grants featured status to an application

**JSON Structure:**
\`\`\`json
{
  "dso": "<party_id>",
  "provider": "<party_id>"
}
\`\`\`

---

## Common Field Types

| Type | Description | Example |
|------|-------------|---------|
| Party ID | Unique party identifier | DSO::1220abcd... |
| Contract ID | Unique contract instance | 00abc123def... |
| Round Number | Mining round object | { "number": "79500" } |
| Numeric Amount | High-precision decimal | "1000.0000000000" |
| Timestamp | ISO 8601 UTC | "2026-01-21T20:00:00Z" |

---

*Auto-generated documentation for Canton Network ACS data.*
`;

const TemplateDocumentation = () => {
  const [copied, setCopied] = useState(false);
  const { toast } = useToast();

  const downloadMarkdown = () => {
    const blob = new Blob([DOCUMENTATION_CONTENT], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'canton-template-documentation.md';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    toast({ title: "Downloaded!", description: "Template documentation saved as Markdown" });
  };

  const downloadWord = () => {
    // Create HTML content that Word can open
    const htmlContent = `
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Canton Network Template Documentation</title>
<style>
body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
h1 { color: #1a1a2e; border-bottom: 2px solid #4361ee; }
h2 { color: #2d3748; margin-top: 30px; }
h3 { color: #4a5568; }
pre { background: #f7fafc; padding: 15px; border-radius: 5px; overflow-x: auto; }
code { background: #edf2f7; padding: 2px 6px; border-radius: 3px; }
table { border-collapse: collapse; width: 100%; margin: 15px 0; }
th, td { border: 1px solid #e2e8f0; padding: 10px; text-align: left; }
th { background: #f7fafc; }
</style>
</head>
<body>
${DOCUMENTATION_CONTENT
  .replace(/^# (.+)$/gm, '<h1>$1</h1>')
  .replace(/^## (.+)$/gm, '<h2>$1</h2>')
  .replace(/^### (.+)$/gm, '<h3>$1</h3>')
  .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
  .replace(/\`\`\`json\n([\s\S]*?)\n\`\`\`/g, '<pre><code>$1</code></pre>')
  .replace(/\`([^`]+)\`/g, '<code>$1</code>')
  .replace(/^- (.+)$/gm, '<li>$1</li>')
  .replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>')
  .replace(/\n\n/g, '</p><p>')
  .replace(/---/g, '<hr>')}
</body>
</html>`;
    
    const blob = new Blob([htmlContent], { type: 'application/msword' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'canton-template-documentation.doc';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    toast({ title: "Downloaded!", description: "Template documentation saved as Word document" });
  };

  const copyToClipboard = async () => {
    await navigator.clipboard.writeText(DOCUMENTATION_CONTENT);
    setCopied(true);
    toast({ title: "Copied!", description: "Documentation copied to clipboard" });
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-bold mb-2">Template Documentation</h2>
          <p className="text-muted-foreground">
            Download comprehensive documentation for all Canton Network templates
          </p>
        </div>

        <Card className="glass-card p-6">
          <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between mb-6">
            <div className="flex items-center gap-3">
              <FileText className="h-8 w-8 text-primary" />
              <div>
                <h3 className="font-semibold">Canton Network Template Documentation</h3>
                <p className="text-sm text-muted-foreground">30+ templates with JSON structure and field descriptions</p>
              </div>
            </div>
            <div className="flex gap-2 flex-wrap">
              <Button onClick={downloadMarkdown} variant="outline" className="flex items-center gap-2">
                <Download className="h-4 w-4" />
                Download .md
              </Button>
              <Button onClick={downloadWord} className="flex items-center gap-2">
                <Download className="h-4 w-4" />
                Download .doc
              </Button>
              <Button onClick={copyToClipboard} variant="secondary" className="flex items-center gap-2">
                {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                {copied ? "Copied!" : "Copy"}
              </Button>
            </div>
          </div>

          <div className="bg-muted/30 rounded-lg p-6 max-h-[600px] overflow-y-auto">
            <pre className="text-sm font-mono whitespace-pre-wrap">{DOCUMENTATION_CONTENT}</pre>
          </div>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default TemplateDocumentation;
