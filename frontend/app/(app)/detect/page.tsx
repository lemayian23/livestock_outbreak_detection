"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { toast } from "sonner";
import { Loader2, Play } from "lucide-react";

import { useAuth } from "@/lib/auth";
import * as api from "@/lib/api";
import { ApiError } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";

const SAMPLE_JSON = `[
  {
    "farm_id": "FARM-0001",
    "date": "2026-09-20",
    "animal_type": "cattle",
    "total_animals": 120,
    "sick_animals": 4,
    "deceased_animals": 1,
    "avg_temperature": 38.6,
    "feed_intake_percent": 88,
    "water_intake_percent": 92,
    "activity_level": 7.2
  }
]`;

export default function DetectPage() {
  const router = useRouter();
  const { token } = useAuth();
  const [submitting, setSubmitting] = useState(false);

  // Single record form
  const [form, setForm] = useState({
    farm_id: "FARM-0001",
    date: new Date().toISOString().slice(0, 10),
    animal_type: "cattle",
    total_animals: 100,
    sick_animals: 3,
    deceased_animals: 0,
    avg_temperature: 38.5,
    feed_intake_percent: 90,
    water_intake_percent: 90,
    activity_level: 7,
  });

  // Paste JSON
  const [json, setJson] = useState(SAMPLE_JSON);

  async function submitRecords(records: Record<string, unknown>[]) {
    if (!token) {
      toast.error("Not authenticated");
      return;
    }
    setSubmitting(true);
    try {
      const res = await api.detect(token, records);
      toast.success(
        `Run complete — ${res.anomalies_detected} anomalies detected`,
      );
      router.push(`/runs/${res.run_id}`);
    } catch (err) {
      const message =
        err instanceof ApiError ? err.message : "Detection failed";
      toast.error(message);
    } finally {
      setSubmitting(false);
    }
  }

  async function submitSingle(e: React.FormEvent) {
    e.preventDefault();
    await submitRecords([form]);
  }

  async function submitJSON() {
    try {
      const parsed = JSON.parse(json);
      const records = Array.isArray(parsed) ? parsed : [parsed];
      await submitRecords(records);
    } catch {
      toast.error("Invalid JSON");
    }
  }

  return (
    <div className="space-y-6 max-w-4xl">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">New detection</h1>
        <p className="text-sm text-muted-foreground">
          Submit livestock health records to check for anomalies.
        </p>
      </div>

      <Tabs defaultValue="single">
        <TabsList>
          <TabsTrigger value="single">Single record</TabsTrigger>
          <TabsTrigger value="json">Paste JSON</TabsTrigger>
        </TabsList>

        <TabsContent value="single">
          <Card>
            <CardHeader>
              <CardTitle>Health record</CardTitle>
              <CardDescription>
                Enter one record. All fields are required unless marked optional.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <form onSubmit={submitSingle} className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="farm_id">Farm ID</Label>
                  <Input
                    id="farm_id"
                    value={form.farm_id}
                    onChange={(e) =>
                      setForm({ ...form, farm_id: e.target.value })
                    }
                    required
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="date">Date</Label>
                  <Input
                    id="date"
                    type="date"
                    value={form.date}
                    onChange={(e) => setForm({ ...form, date: e.target.value })}
                    required
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="animal_type">Animal type</Label>
                  <Input
                    id="animal_type"
                    value={form.animal_type}
                    onChange={(e) =>
                      setForm({ ...form, animal_type: e.target.value })
                    }
                    required
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="total_animals">Total animals</Label>
                  <Input
                    id="total_animals"
                    type="number"
                    value={form.total_animals}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        total_animals: Number(e.target.value),
                      })
                    }
                    required
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="sick_animals">Sick animals</Label>
                  <Input
                    id="sick_animals"
                    type="number"
                    value={form.sick_animals}
                    onChange={(e) =>
                      setForm({ ...form, sick_animals: Number(e.target.value) })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="deceased_animals">Deceased animals</Label>
                  <Input
                    id="deceased_animals"
                    type="number"
                    value={form.deceased_animals}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        deceased_animals: Number(e.target.value),
                      })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="avg_temperature">Avg temperature (°C)</Label>
                  <Input
                    id="avg_temperature"
                    type="number"
                    step="0.1"
                    value={form.avg_temperature}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        avg_temperature: Number(e.target.value),
                      })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="activity_level">Activity level</Label>
                  <Input
                    id="activity_level"
                    type="number"
                    step="0.1"
                    value={form.activity_level}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        activity_level: Number(e.target.value),
                      })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="feed">Feed intake (%)</Label>
                  <Input
                    id="feed"
                    type="number"
                    value={form.feed_intake_percent}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        feed_intake_percent: Number(e.target.value),
                      })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="water">Water intake (%)</Label>
                  <Input
                    id="water"
                    type="number"
                    value={form.water_intake_percent}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        water_intake_percent: Number(e.target.value),
                      })
                    }
                  />
                </div>
                <div className="sm:col-span-2">
                  <Button type="submit" disabled={submitting}>
                    {submitting ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                    Run detection
                  </Button>
                </div>
              </form>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="json">
          <Card>
            <CardHeader>
              <CardTitle>Paste JSON records</CardTitle>
              <CardDescription>
                An array of objects with the same schema as the form.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <Textarea
                rows={14}
                value={json}
                onChange={(e) => setJson(e.target.value)}
              />
              <Button onClick={submitJSON} disabled={submitting}>
                {submitting && <Loader2 className="h-4 w-4 animate-spin" />}
                Run detection
              </Button>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}