"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Activity, AlertTriangle, Gauge, Timer } from "lucide-react";

import { useAuth } from "@/lib/auth";
import * as api from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";

export default function DashboardPage() {
  const { token, user } = useAuth();
  const [summary, setSummary] = useState<{
    total: number;
    anomalies: number;
    avgQuality: number | null;
    avgDuration: number | null;
  } | null>(null);
  const [recent, setRecent] = useState<api.RunSummary[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!token) return;
    (async () => {
      try {
        const res = await api.listRuns(token, { limit: 10 });
        setRecent(res.items);
        const total = res.total;
        const anomalies = res.items.reduce(
          (s, r) => s + r.anomalies_detected,
          0,
        );
        const qualities = res.items
          .map((r) => r.quality_score)
          .filter((q): q is number => typeof q === "number");
        const durations = res.items
          .map((r) => r.duration_ms)
          .filter((d): d is number => typeof d === "number");
        setSummary({
          total,
          anomalies,
          avgQuality: qualities.length
            ? qualities.reduce((a, b) => a + b, 0) / qualities.length
            : null,
          avgDuration: durations.length
            ? durations.reduce((a, b) => a + b, 0) / durations.length
            : null,
        });
      } finally {
        setLoading(false);
      }
    })();
  }, [token]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
          <p className="text-sm text-muted-foreground">
            Welcome back, {user?.full_name || user?.email}
          </p>
        </div>
        <Button asChild>
          <Link href="/detect">New detection</Link>
        </Button>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          icon={<Activity className="h-4 w-4" />}
          label="Total runs"
          value={loading ? null : summary?.total ?? 0}
        />
        <StatCard
          icon={<AlertTriangle className="h-4 w-4" />}
          label="Anomalies (recent)"
          value={loading ? null : summary?.anomalies ?? 0}
        />
        <StatCard
          icon={<Gauge className="h-4 w-4" />}
          label="Avg quality"
          value={
            loading || summary?.avgQuality == null
              ? null
              : summary.avgQuality.toFixed(3)
          }
        />
        <StatCard
          icon={<Timer className="h-4 w-4" />}
          label="Avg duration"
          value={
            loading || summary?.avgDuration == null
              ? null
              : `${summary.avgDuration.toFixed(0)} ms`
          }
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Recent runs</CardTitle>
          <CardDescription>Your last 10 detection runs</CardDescription>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          ) : recent.length === 0 ? (
            <div className="py-8 text-center text-sm text-muted-foreground">
              No runs yet.{" "}
              <Link href="/detect" className="text-primary hover:underline">
                Submit your first detection
              </Link>
              .
            </div>
          ) : (
            <div className="divide-y">
              {recent.map((r) => (
                <Link
                  key={r.id}
                  href={`/runs/${r.id}`}
                  className="flex items-center justify-between py-3 hover:bg-accent/50 -mx-2 px-2 rounded"
                >
                  <div className="flex flex-col">
                    <span className="text-sm font-medium">
                      {new Date(r.created_at).toLocaleString()}
                    </span>
                    <span className="text-xs text-muted-foreground">
                      {r.records_processed} records · {r.duration_ms?.toFixed(0) ?? "—"} ms
                    </span>
                  </div>
                  <div className="flex items-center gap-3">
                    <Badge
                      variant={
                        r.anomalies_detected > 0 ? "warning" : "success"
                      }
                    >
                      {r.anomalies_detected} anomalies
                    </Badge>
                    <Badge variant="outline">{r.status}</Badge>
                  </div>
                </Link>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function StatCard({
  icon,
  label,
  value,
}: {
  icon: React.ReactNode;
  label: string;
  value: string | number | null;
}) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">
          {label}
        </CardTitle>
        <span className="text-muted-foreground">{icon}</span>
      </CardHeader>
      <CardContent>
        {value === null ? (
          <Skeleton className="h-8 w-16" />
        ) : (
          <div className="text-2xl font-semibold">{value}</div>
        )}
      </CardContent>
    </Card>
  );
}