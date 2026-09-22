import Link from "next/link";
import { Activity } from "lucide-react";

export default function NotFound() {
  return (
    <main className="min-h-screen flex items-center justify-center bg-gradient-to-b from-background to-muted">
      <div className="text-center px-4">
        <div className="mb-6 inline-flex h-16 w-16 items-center justify-center rounded-full bg-primary/10 text-primary">
          <Activity className="h-8 w-8" />
        </div>
        <h1 className="text-6xl font-bold tracking-tight">404</h1>
        <p className="mt-4 text-muted-foreground">
          This page doesn&apos;t exist or has moved.
        </p>
        <Link
          href="/"
          className="mt-8 inline-flex items-center rounded-md bg-primary px-6 py-3 text-sm font-medium text-primary-foreground hover:opacity-90"
        >
          Go home
        </Link>
      </div>
    </main>
  );
}