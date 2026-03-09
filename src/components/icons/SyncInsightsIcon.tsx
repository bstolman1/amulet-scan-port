import { SVGProps } from "react";

export function SyncInsightsIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <text
        x="12"
        y="16"
        textAnchor="middle"
        fontSize="14"
        fontWeight="600"
        fontFamily="-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Arial, sans-serif"
        fill="currentColor"
      >
        SI
      </text>
    </svg>
  );
}
