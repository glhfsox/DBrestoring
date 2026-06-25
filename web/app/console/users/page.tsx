import { requireAdmin } from "@/lib/auth";
import { listUsers } from "@/lib/db";
import { UserManager } from "@/components/console/UserManager";

export const dynamic = "force-dynamic";

export default async function UsersPage() {
  const session = requireAdmin();
  const users = await listUsers();

  return (
    <div>
      <h1 className="text-2xl font-bold text-white">Users</h1>
      <p className="mt-1 max-w-2xl text-sm text-zinc-500">
        Console accounts and roles. <span className="text-zinc-300">admin</span> can manage tokens
        and users; <span className="text-zinc-300">viewer</span> can only see the dashboard. Passwords
        are stored hashed (scrypt).
      </p>
      <div className="mt-6">
        <UserManager users={users} currentUser={session.sub} />
      </div>
    </div>
  );
}
