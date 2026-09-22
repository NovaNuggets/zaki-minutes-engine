/** Server-side egress for any request carrying a deployment or user credential.
 *
 * Fetch follows redirects by default and may replay custom auth headers and request bodies. A
 * configured upstream is one authority boundary, so credential-bearing calls always reject a
 * Location instead of delegating trust to it. */
export function credentialedFetch(
  input: RequestInfo | URL,
  init: RequestInit = {},
): Promise<Response> {
  return fetch(input, { ...init, redirect: "error" });
}
