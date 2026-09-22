"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { toast } from "sonner";
import { ArrowLeft, Loader2 } from "lucide-react";

import { useAuth } from "@/lib/auth";
import * as api from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { SeverityBadge } from "@/components/severity-badge";

export default function RunDetailPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const { token } = useAuth();
  const [run, setRun] = useState<api.RunDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!token || !id) return;
    (async () => {
      try {
        const r = await api.getRun(token, id);
        setRun(r);
      } catch (err) {
        toast.error("Could not load run");
        router.replace("/runs");
      } finally {
        setLoading(false);
      }
    })();
  }, [token, id, router]);

  async function handleDelete() {
    if (!token || !id) return;
    if (!confirm("Delete this run? This cannot be undone.")) return;
    try {
      await api.deleteRun(token, id);
      toast.success("Run deleted");
      router.replace("/runs");
    } catch {
      toast.error("Delete failed");
    }
  }

  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-32 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (!run) return null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="sm" asChild>
            <Link href="/runs">
              <ArrowLeft className="h-4 w-4" />
              Back
            </Link>
          </Button>
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">
              Run details
            </h1>
            <p className="text-sm text-muted-foreground font-mono">{run.id}</p>
          </div>
        </div>
        <Button variant="destructive" size="sm" onClick={handleDelete}>
          Delete
        </Button>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <SummaryStat label="Records" value={run.records_processed} />
        <SummaryStat label="Anomalies" value={run.anomalies_detected} />
        <SummaryStat
          label="Quality"
          value={run.quality_score != null ? run.quality_score.toFixed(3) : "—"}
        />
        <SummaryStat
          label="Duration"
          value={run.duration_ms != null ? `${run.duration_ms.toFixed(0)} ms` : "—"}
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Anomalies detected</CardTitle>
          <CardDescription>
            {run.anomalies.length === 0
              ? "No anomalies detected"
              : `${run.anomalies.length} anomalies`}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {run.anomalies.length === 0 ? (
            <p className="py-6 text-center text-sm text-muted-foreground">
              ✅ All records passed the anomaly detector.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Farm</TableHead>
                  <TableHead>Animal</TableHead>
                  <TableHead>Date</TableHead>
                  <TableHead>Severity</TableHead>
                  <TableHead>Score</TableHead>
                  <TableHead>Description</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {run.anomalies.map((a, i) => (
                  <TableRow key={i}>
                    <TableCell>{a.farm_id ?? "—"}</TableCell>
                    <TableCell>{a.animal_type ?? "—"}</TableCell>
                    <TableCell>{a.date ?? "—"}</TableCell>
                    <TableCell>
                      <SeverityBadge severity={a.severity} />
                    </TableCell>
                    <TableCell>
                      {a.score != null ? a.score.toFixed(3) : "—"}
                    </TableCell>
                    <TableCell className="max-w-md truncate">
                      {a.description ?? "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Metadata</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <Row label="Status">
            <Badge variant="outline">{run.status}</Badge>
          </Row>
          <Row label="Created">{new Date(run.created_at).toLocaleString()}</Row>
          {run.completed_at && (
            <Row label="Completed">
              {new Date(run.completed_at).toLocaleString()}
            </Row>
          )}
          {run.error_message && (
            <Row label="Error">
              <span className="text-destructive">{run.error_message}</span>
            </Row>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function SummaryStat({
  label,
  value,
}: {
  label: string;
  value: string | number;
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">
          {label}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-semibold">{value}</div>
      </CardContent>
    </Card>
  );
}

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-center gap-4">
      <span className="w-24 text-muted-foreground">{label}</span>
      <span>{children}</span>
    </div>
  );
}