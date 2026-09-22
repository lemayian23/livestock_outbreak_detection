import { Badge } from "@/components/ui/badge";

export function SeverityBadge({ severity }: { severity?: string | null }) {
  const s = (severity ?? "").toLowerCase();
  const variant: "destructive" | "warning" | "secondary" | "success" =
    s === "critical" || s === "high"
      ? "destructive"
      : s === "medium"
        ? "warning"
        : s === "low"
          ? "success"
          : "secondary";
  return <Badge variant={variant}>{(severity ?? "unknown").toUpperCase()}</Badge>;
}