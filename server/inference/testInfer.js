import { inferStage } from "./inferStage.js";

(async () => {
  const result = await inferStage(
    "CIP-0123 Vote Passed",
    "The proposal has been approved by the community."
  );

  console.log(result);
})();
