import { site } from "./site";

export type Tier = {
  id: "community" | "team" | "enterprise";
  name: string;
  price: string;
  cadence?: string;
  tagline: string;
  cta: { label: string; href: string };
  highlighted?: boolean;
  features: string[];
};

export const tiers: Tier[] = [
  {
    id: "community",
    name: "Community",
    price: "$0",
    cadence: "forever",
    tagline: "Self-hosted and open source. Everything you need to protect your own databases.",
    cta: { label: "Get started", href: site.github },
    features: [
      "PostgreSQL, MySQL/MariaDB, MongoDB, SQLite",
      "Full, differential & incremental backups",
      "AES-256-GCM encryption at rest",
      "Local + S3-compatible storage",
      "systemd / launchd scheduling",
      "Retention policies & restore verification",
      "Community support on GitHub",
      "MIT-licensed core",
    ],
  },
  {
    id: "team",
    name: "Team",
    price: "$99",
    cadence: "/month",
    tagline: "Centralized control, dashboards, and monitoring across all of your servers.",
    cta: { label: "Contact sales", href: "/contact?plan=team" },
    highlighted: true,
    features: [
      "Everything in Community",
      "Web dashboard for every backup",
      "Centralized multi-server management",
      "Hosted monitoring & failure alerting",
      "Role-based access control (RBAC)",
      "Google SSO",
      "Email support, next business day",
    ],
  },
  {
    id: "enterprise",
    name: "Enterprise",
    price: "Custom",
    tagline: "Compliance, SLAs, and white-glove support for business-critical data.",
    cta: { label: "Contact sales", href: "/contact?plan=enterprise" },
    features: [
      "Everything in Team",
      "Support SLA & a dedicated engineer",
      "Audit logs & compliance reporting",
      "SAML SSO & SCIM provisioning",
      "Air-gapped / on-prem deployment",
      "Custom integrations & priority roadmap",
      "Commercial license & indemnification",
    ],
  },
];
