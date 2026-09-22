"use client";

import { useEffect, useState } from "react";
import { toast } from "sonner";
import { Copy, Loader2, Plus, Trash2 } from "lucide-react";

import { useAuth } from "@/lib/auth";
import * as api from "@/lib/api";
import { ApiError } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";

export default function SettingsPage() {
  const { user, token } = useAuth();
  const [keys, setKeys] = useState<api.ApiKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [newKeyName, setNewKeyName] = useState("default");
  const [showCreate, setShowCreate] = useState(false);
  const [createdKey, setCreatedKey] = useState<string | null>(null);

  useEffect(() => {
    if (!token) return;
    (async () => {
      try {
        const list = await api.listApiKeys(token);
        setKeys(list);
      } finally {
        setLoading(false);
      }
    })();
  }, [token]);

  async function handleCreate() {
    if (!token) return;
    setCreating(true);
    try {
      const res = await api.createApiKey(token, newKeyName);
      setCreatedKey(res.key);
      setShowCreate(false);
      setNewKeyName("default");
      const list = await api.listApiKeys(token);
      setKeys(list);
    } catch (err) {
      toast.error(
        err instanceof ApiError ? err.message : "Could not create key",
      );
    } finally {
      setCreating(false);
    }
  }

  async function handleRevoke(id: string) {
    if (!token) return;
    if (!confirm("Revoke this key? Applications using it will stop working."))
      return;
    try {
      await api.revokeApiKey(token, id);
      setKeys(keys.map((k) => (k.id === id ? { ...k, revoked_at: "now" } : k)));
      toast.success("Key revoked");
    } catch {
      toast.error("Revoke failed");
    }
  }

  return (
    <div className="space-y-6 max-w-4xl">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
        <p className="text-sm text-muted-foreground">
          Manage your profile and API access.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Profile</CardTitle>
          <CardDescription>Your account information</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          <div className="flex items-center gap-3">
            <span className="w-28 text-muted-foreground">Email</span>
            <span>{user?.email}</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="w-28 text-muted-foreground">Name</span>
            <span>{user?.full_name || "—"}</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="w-28 text-muted-foreground">Organization</span>
            <span>{user?.organization || "—"}</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="w-28 text-muted-foreground">Role</span>
            <Badge variant="outline">{user?.role}</Badge>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle>API keys</CardTitle>
            <CardDescription>
              Use these to call the API from external services
            </CardDescription>
          </div>
          <Button onClick={() => setShowCreate(true)}>
            <Plus className="h-4 w-4" />
            New key
          </Button>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="space-y-2">
              {[1, 2].map((i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          ) : keys.length === 0 ? (
            <p className="py-6 text-center text-sm text-muted-foreground">
              No API keys yet. Create one to get started.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Prefix</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead>Last used</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {keys.map((k) => (
                  <TableRow key={k.id}>
                    <TableCell>{k.name || "—"}</TableCell>
                    <TableCell className="font-mono text-xs">
                      {k.prefix}…
                    </TableCell>
                    <TableCell>
                      {new Date(k.created_at).toLocaleDateString()}
                    </TableCell>
                    <TableCell>
                      {k.last_used_at
                        ? new Date(k.last_used_at).toLocaleDateString()
                        : "Never"}
                    </TableCell>
                    <TableCell>
                      {k.revoked_at ? (
                        <Badge variant="destructive">revoked</Badge>
                      ) : (
                        <Badge variant="success">active</Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      {!k.revoked_at && (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleRevoke(k.id)}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create API key</DialogTitle>
            <DialogDescription>
              Give your key a name to remember what it's used for.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <Label htmlFor="keyName">Name</Label>
            <Input
              id="keyName"
              value={newKeyName}
              onChange={(e) => setNewKeyName(e.target.value)}
              placeholder="e.g. production-server"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowCreate(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreate} disabled={creating}>
              {creating && <Loader2 className="h-4 w-4 animate-spin" />}
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!createdKey} onOpenChange={() => setCreatedKey(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Your new API key</DialogTitle>
            <DialogDescription>
              Copy this now. You won't be able to see it again.
            </DialogDescription>
          </DialogHeader>
          <div className="rounded-md bg-muted p-3 font-mono text-xs break-all">
            {createdKey}
          </div>
          <DialogFooter>
            <Button
              onClick={() => {
                navigator.clipboard.writeText(createdKey ?? "");
                toast.success("Copied to clipboard");
              }}
            >
              <Copy className="h-4 w-4" />
              Copy
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}