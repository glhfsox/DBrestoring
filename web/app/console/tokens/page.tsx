import { requireAdmin } from "@/lib/auth";
import { listAgentTokens } from "@/lib/db";
import { TokenManager } from "@/components/console/TokenManager";

export const dynamic = "force-dynamic";

export default async function TokensPage() {
  requireAdmin();
  const tokens = await listAgentTokens();

  return (
    <div>
      <h1 className="text-2xl font-bold text-white">Agent tokens</h1>
      <p className="mt-1 max-w-2xl text-sm text-zinc-500">
        Each server should use its own token in <code className="text-brand-300">control_plane.token</code>.
        Tokens are stored hashed and can be revoked individually — far safer than one shared token.
      </p>
      <div className="mt-6">
        <TokenManager tokens={tokens} />
      </div>
    </div>
  );
}
