import { NextResponse } from "next/server";

import { readBoundedText } from "../boundedBody";
import { MAX_ADMIN_PROXY_RESPONSE_BYTES } from "../proxyLimits";

const NO_STORE = { "Cache-Control": "no-store" } as const;

/** Bound and validate the admin panel's internal agent-api JSON without reflecting error bodies. */
export async function adminProxyResponse(upstream: Response): Promise<Response> {
  if (upstream.status === 204 || upstream.status === 205 || upstream.status === 304) {
    await upstream.body?.cancel("null-body status").catch(() => undefined);
    return new Response(null, { status: upstream.status, headers: NO_STORE });
  }
  if (!upstream.ok) {
    await upstream.body?.cancel("untrusted agent-api error").catch(() => undefined);
    return NextResponse.json(
      { error: "agent-api request failed", status: upstream.status },
      { status: upstream.status, headers: NO_STORE },
    );
  }
  const raw = await readBoundedText(upstream, MAX_ADMIN_PROXY_RESPONSE_BYTES);
  if (raw === null) {
    return NextResponse.json(
      { error: "agent-api response is too large" },
      { status: 502, headers: NO_STORE },
    );
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (parsed === null || typeof parsed !== "object") throw new Error("invalid shape");
    return NextResponse.json(parsed, { status: upstream.status, headers: NO_STORE });
  } catch {
    return NextResponse.json(
      { error: "agent-api returned an invalid response" },
      { status: 502, headers: NO_STORE },
    );
  }
}
