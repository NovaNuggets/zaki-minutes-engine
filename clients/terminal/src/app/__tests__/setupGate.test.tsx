/** SetupGate — the admin first-run wizard's gating behavior. The wizard must show ONLY to an
 *  admin on an instance whose setup is incomplete; everyone else falls straight through to the
 *  workbench (children). The probe is /api/admin/settings/setup — 404 (non-admin) → null. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";

import { SetupGate, shouldShowSetup } from "../SetupGate";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function stubSetupProbe(response: { status: number; value?: Record<string, string> }) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      if (String(url).includes("/api/admin/settings/setup")) {
        if (response.status === 404) return new Response(null, { status: 404 });
        return new Response(JSON.stringify({ key: "setup", value: response.value ?? {} }), { status: 200 });
      }
      // the wizard's mount-time model detection — irrelevant to gating, keep it quiet
      return new Response(JSON.stringify({ ok: false, summary: "stub" }), { status: 200 });
    }),
  );
}

describe("shouldShowSetup", () => {
  it("null (non-admin probe 404) → hidden", () => expect(shouldShowSetup(null)).toBe(false));
  it("completed → hidden", () => expect(shouldShowSetup({ completed: "true" })).toBe(false));
  it("fresh / partial → shown", () => {
    expect(shouldShowSetup({})).toBe(true);
    expect(shouldShowSetup({ models: "done" })).toBe(true);
  });
});

describe("SetupGate", () => {
  it("non-admin falls through to the workbench", async () => {
    stubSetupProbe({ status: 404 });
    render(<SetupGate><div data-testid="workbench" /></SetupGate>);
    await waitFor(() => expect(screen.getByTestId("workbench")).toBeTruthy());
  });

  it("completed instance falls through", async () => {
    stubSetupProbe({ status: 200, value: { completed: "true" } });
    render(<SetupGate><div data-testid="workbench" /></SetupGate>);
    await waitFor(() => expect(screen.getByTestId("workbench")).toBeTruthy());
  });

  it("admin on a fresh instance gets the wizard, not the workbench", async () => {
    stubSetupProbe({ status: 200, value: {} });
    render(<SetupGate><div data-testid="workbench" /></SetupGate>);
    await waitFor(() => expect(screen.getByText("How should the agent think?")).toBeTruthy());
    expect(screen.queryByTestId("workbench")).toBeNull();
  });

  it("treats operator model setup as saved configuration without claiming a live test", async () => {
    let modelStatusReads = 0;
    let modelWrites = 0;
    vi.stubGlobal("fetch", vi.fn(async (url: string, init?: RequestInit) => {
      const path = String(url);
      if (path.includes("/api/admin/settings/setup")) {
        return new Response(JSON.stringify({ key: "setup", value: {} }), { status: 200 });
      }
      if (path.includes("/api/admin/settings/models") && init?.method === "PUT") {
        modelWrites++;
        return new Response(JSON.stringify({ key: "models", value: { mode: "subscription" } }), { status: 200 });
      }
      if (path.includes("/api/models/test")) {
        modelStatusReads++;
        return new Response(JSON.stringify({ ok: false, summary: "No operator model configuration saved." }), { status: 200 });
      }
      return new Response("{}", { status: 200 });
    }));

    render(<SetupGate><div data-testid="workbench" /></SetupGate>);
    await screen.findByText("How should the agent think?");
    fireEvent.click(screen.getByText("OpenRouter or custom endpoint"));
    fireEvent.change(screen.getByPlaceholderText("https://openrouter.ai/api/v1"), {
      target: { value: "https://models.example.test/v1" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save configuration" }));

    await screen.findByText(/Operator model configuration saved/i);
    expect(modelWrites).toBe(1);
    expect(modelStatusReads).toBe(1);
    expect(screen.queryByText(/configured and tested/i)).toBeNull();
  });

  it("does not treat a personal model configuration as operator-managed setup", async () => {
    vi.stubGlobal("fetch", vi.fn(async (url: string) => {
      const path = String(url);
      if (path.includes("/api/admin/settings/setup")) {
        return new Response(JSON.stringify({ key: "setup", value: {} }), { status: 200 });
      }
      if (path.includes("/api/models/test")) {
        return new Response(JSON.stringify({
          ok: true,
          summary: "Personal model configuration is available.",
          source: "user",
          managed: false,
        }), { status: 200 });
      }
      return new Response("{}", { status: 200 });
    }));

    render(<SetupGate><div data-testid="workbench" /></SetupGate>);
    await screen.findByText("not configured");

    expect((screen.getByRole("button", { name: "Continue" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: "Save configuration" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("probe failure fails SAFE — workbench renders", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("ECONNREFUSED"); }));
    render(<SetupGate><div data-testid="workbench" /></SetupGate>);
    await waitFor(() => expect(screen.getByTestId("workbench")).toBeTruthy());
  });
});
