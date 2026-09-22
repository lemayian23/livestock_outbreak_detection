import Link from "next/link";
import { Activity, ShieldCheck, Zap } from "lucide-react";

export default function LandingPage() {
  return (
    <main className="min-h-screen bg-gradient-to-b from-background to-muted">
      <header className="container flex h-16 items-center justify-between">
        <div className="flex items-center gap-2 font-semibold">
          <Activity className="h-5 w-5 text-primary" />
          <span>Livestock Guard</span>
        </div>
        <nav className="flex items-center gap-3">
          <Link
            href="/login"
            className="text-sm font-medium hover:text-primary transition-colors"
          >
            Sign in
          </Link>
          <Link
            href="/signup"
            className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:opacity-90 transition-opacity"
          >
            Get started
          </Link>
        </nav>
      </header>

      <section className="container mx-auto max-w-3xl py-24 text-center">
        <h1 className="text-5xl font-bold tracking-tight sm:text-6xl">
          Catch livestock outbreaks
          <span className="text-primary"> before they spread.</span>
        </h1>
        <p className="mt-6 text-lg text-muted-foreground">
          Submit farm health records. Get anomalies, quality scores, and
          actionable alerts in seconds — powered by ensemble machine learning.
        </p>
        <div className="mt-10 flex justify-center gap-3">
          <Link
            href="/signup"
            className="rounded-md bg-primary px-6 py-3 font-medium text-primary-foreground hover:opacity-90 transition-opacity"
          >
            Start free
          </Link>
          <Link
            href="/login"
            className="rounded-md border px-6 py-3 font-medium hover:bg-muted transition-colors"
          >
            Sign in
          </Link>
        </div>
      </section>

      <section className="container mx-auto grid max-w-5xl gap-6 py-16 sm:grid-cols-3">
        <FeatureCard
          icon={<Zap className="h-5 w-5" />}
          title="Real-time detection"
          description="Ensemble anomaly detection flags sick animals within seconds of submission."
        />
        <FeatureCard
          icon={<ShieldCheck className="h-5 w-5" />}
          title="Data quality scoring"
          description="Every submission is validated against a schema with quality grades A–F."
        />
        <FeatureCard
          icon={<Activity className="h-5 w-5" />}
          title="Actionable alerts"
          description="Severity-ranked anomalies you can filter, download, and act on."
        />
      </section>

      <footer className="container py-8 text-center text-sm text-muted-foreground">
        Built with FastAPI · Next.js · Neon Postgres
      </footer>
    </main>
  );
}

function FeatureCard({
  icon,
  title,
  description,
}: {
  icon: React.ReactNode;
  title: string;
  description: string;
}) {
  return (
    <div className="rounded-lg border bg-card p-6">
      <div className="mb-3 inline-flex h-10 w-10 items-center justify-center rounded-md bg-primary/10 text-primary">
        {icon}
      </div>
      <h3 className="font-semibold">{title}</h3>
      <p className="mt-2 text-sm text-muted-foreground">{description}</p>
    </div>
  );
}