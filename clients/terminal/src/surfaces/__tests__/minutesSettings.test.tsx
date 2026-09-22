import React from "react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const minutes = vi.hoisted(() => ({
  get: vi.fn(),
  set: vi.fn(),
}));

vi.mock("../../contributions", () => ({ registerTab: vi.fn() }));
vi.mock("../minutesApi", () => ({
  getMinutesConfig: minutes.get,
  setMinutesConfig: minutes.set,
}));

import { MinutesSection } from "../settings";

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("Minutes settings", () => {
  it("saves user consent, agent-read opt-in, and per-scope retention separately from operator policy", async () => {
    const config = {
      operator_enabled: true,
      capture_enabled: false,
      agent_read_enabled: false,
      policy_version: "minutes-capture.v1",
      attested_at: "2026-07-15T11:00:00Z",
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    };
    minutes.get.mockResolvedValue(config);
    minutes.set.mockImplementation(async (update) => ({ ...config, ...update }));

    render(<MinutesSection />);

    expect(await screen.findByText(/available from your operator/i)).toBeTruthy();
    fireEvent.click(screen.getByLabelText(/allow meeting capture/i));
    fireEvent.click(screen.getByLabelText(/allow my agent to read/i));
    fireEvent.change(screen.getByLabelText(/audio retention/i), { target: { value: "1" } });
    fireEvent.change(screen.getByLabelText(/transcript retention/i), { target: { value: "14" } });
    fireEvent.change(screen.getByLabelText(/summary retention/i), { target: { value: "14" } });
    fireEvent.click(screen.getByRole("button", { name: /save minutes settings/i }));

    await waitFor(() => expect(minutes.set).toHaveBeenCalledWith({
      capture_enabled: true,
      agent_read_enabled: true,
      retention_days: { audio: 1, transcript: 14, summary: 14 },
    }));
  });

  it("does not include consent fields in a retention-only update", async () => {
    const config = {
      operator_enabled: true,
      read_operator_enabled: true,
      capture_enabled: true,
      agent_read_enabled: true,
      capture_requested: true,
      agent_read_requested: true,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    };
    minutes.get.mockResolvedValue(config);
    minutes.set.mockImplementation(async (update) => ({ ...config, ...update }));

    render(<MinutesSection />);
    fireEvent.change(await screen.findByLabelText(/audio retention/i), { target: { value: "3" } });
    fireEvent.click(screen.getByRole("button", { name: /save minutes settings/i }));

    await waitFor(() => expect(minutes.set).toHaveBeenCalledWith({
      retention_days: { audio: 3, transcript: 30, summary: 30 },
    }));
  });

  it("lets stored permissions be revoked during operator rollback and keeps them off after re-enable", async () => {
    const rolledBack = {
      operator_enabled: false,
      read_operator_enabled: false,
      capture_enabled: false,
      agent_read_enabled: false,
      capture_requested: true,
      agent_read_requested: true,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    };
    minutes.get.mockResolvedValueOnce(rolledBack);
    minutes.set.mockResolvedValue({
      ...rolledBack,
      capture_requested: false,
      agent_read_requested: false,
    });

    render(<MinutesSection />);
    const capture = await screen.findByLabelText(/allow meeting capture/i) as HTMLInputElement;
    const read = screen.getByLabelText(/allow my agent to read/i) as HTMLInputElement;
    expect(capture.checked).toBe(true);
    expect(capture.disabled).toBe(false);
    expect(read.checked).toBe(true);
    expect(read.disabled).toBe(false);

    fireEvent.click(capture);
    fireEvent.click(read);
    fireEvent.click(screen.getByRole("button", { name: /save minutes settings/i }));
    await waitFor(() => expect(minutes.set).toHaveBeenCalledWith({
      capture_enabled: false,
      agent_read_enabled: false,
    }));

    cleanup();
    minutes.get.mockResolvedValueOnce({
      ...rolledBack,
      operator_enabled: true,
      read_operator_enabled: true,
      capture_requested: false,
      agent_read_requested: false,
    });
    render(<MinutesSection />);
    expect((await screen.findByLabelText(/allow meeting capture/i) as HTMLInputElement).checked).toBe(false);
    expect((screen.getByLabelText(/allow my agent to read/i) as HTMLInputElement).checked).toBe(false);
  });

  it("locks unavailable capabilities while leaving user-owned retention editable", async () => {
    minutes.get.mockResolvedValue({
      operator_enabled: false,
      capture_enabled: false,
      agent_read_enabled: false,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    });

    render(<MinutesSection />);

    expect(await screen.findByText(/not available from your operator/i)).toBeTruthy();
    expect((screen.getByLabelText(/allow meeting capture/i) as HTMLInputElement).disabled).toBe(true);
    expect((screen.getByLabelText(/allow my agent to read/i) as HTMLInputElement).disabled).toBe(true);
    expect((screen.getByLabelText(/audio retention/i) as HTMLInputElement).disabled).toBe(false);
    expect((screen.getByRole("button", { name: /save minutes settings/i }) as HTMLButtonElement).disabled).toBe(true);
    expect(minutes.set).not.toHaveBeenCalled();
  });

  it("preserves read-only operator availability when a PUT response omits optional operator metadata", async () => {
    minutes.get.mockResolvedValue({
      operator_enabled: true,
      capture_enabled: false,
      agent_read_enabled: false,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    });
    minutes.set.mockResolvedValue({
      capture_enabled: true,
      agent_read_enabled: false,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    });

    render(<MinutesSection />);
    fireEvent.click(await screen.findByLabelText(/allow meeting capture/i));
    fireEvent.click(screen.getByRole("button", { name: /save minutes settings/i }));

    await screen.findByText("Saved");
    expect(screen.getByText(/available from your operator/i).textContent).toMatch(/capture available from your operator/i);
    expect((screen.getByLabelText(/allow meeting capture/i) as HTMLInputElement).disabled).toBe(false);
  });

  it("treats capture and agent-read operator availability as independent controls", async () => {
    minutes.get.mockResolvedValue({
      operator_enabled: true,
      read_operator_enabled: false,
      capture_enabled: false,
      agent_read_enabled: false,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    });

    render(<MinutesSection />);

    expect(await screen.findByText(/capture available/i)).toBeTruthy();
    expect(screen.getByText(/agent reads unavailable/i)).toBeTruthy();
    expect((screen.getByLabelText(/allow meeting capture/i) as HTMLInputElement).disabled).toBe(false);
    expect((screen.getByLabelText(/allow my agent to read/i) as HTMLInputElement).disabled).toBe(true);
    expect((screen.getByLabelText(/audio retention/i) as HTMLInputElement).disabled).toBe(false);
  });

  it("matches the server's complete 1–3650 day retention bounds", async () => {
    minutes.get.mockResolvedValue({
      operator_enabled: false,
      read_operator_enabled: false,
      capture_enabled: false,
      agent_read_enabled: false,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    });
    render(<MinutesSection />);

    const audio = await screen.findByLabelText(/audio retention/i) as HTMLInputElement;
    expect(audio.min).toBe("1");
    expect(audio.max).toBe("3650");
    fireEvent.change(audio, { target: { value: "0" } });
    expect(audio.value).toBe("7");
    expect((screen.getByRole("button", { name: /save minutes settings/i }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("prevents a derived summary or audio source from outliving the transcript", async () => {
    minutes.get.mockResolvedValue({
      operator_enabled: true,
      read_operator_enabled: true,
      capture_enabled: false,
      agent_read_enabled: false,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    });
    render(<MinutesSection />);

    fireEvent.change(await screen.findByLabelText(/summary retention/i), { target: { value: "31" } });

    expect(screen.getByRole("alert").textContent).toMatch(/less than or equal to transcript/i);
    expect((screen.getByRole("button", { name: /save minutes settings/i }) as HTMLButtonElement).disabled).toBe(true);
    expect(minutes.set).not.toHaveBeenCalled();
  });

  it("offers one-click reconsent when capture was requested but its attestation is stale", async () => {
    const config = {
      operator_enabled: true,
      read_operator_enabled: true,
      capture_enabled: false,
      capture_requested: true,
      agent_read_enabled: false,
      agent_read_requested: false,
      capture_repair: "reconsent_required",
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    };
    minutes.get.mockResolvedValue(config);
    minutes.set.mockResolvedValue({ ...config, capture_enabled: true, capture_repair: null });

    render(<MinutesSection />);
    fireEvent.click(await screen.findByRole("button", { name: /re-consent to minutes capture/i }));

    await waitFor(() => expect(minutes.set).toHaveBeenCalledWith({ capture_enabled: true }));
  });

  it("repairs corrupt retention and reconsents in one explicit click", async () => {
    const config = {
      operator_enabled: true,
      read_operator_enabled: true,
      capture_enabled: false,
      capture_requested: true,
      agent_read_enabled: false,
      agent_read_requested: false,
      capture_repair: "retention_repair_required",
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    };
    minutes.get.mockResolvedValue(config);
    minutes.set.mockResolvedValue({ ...config, capture_enabled: true, capture_repair: null });

    render(<MinutesSection />);
    fireEvent.click(await screen.findByRole("button", { name: /repair retention and re-consent/i }));

    await waitFor(() => expect(minutes.set).toHaveBeenCalledWith({
      capture_enabled: true,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    }));
  });

  it("does not submit a retention repair after the displayed policy becomes invalid", async () => {
    minutes.get.mockResolvedValue({
      operator_enabled: true,
      read_operator_enabled: true,
      capture_enabled: false,
      capture_requested: true,
      agent_read_enabled: false,
      agent_read_requested: false,
      capture_repair: "retention_repair_required",
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    });

    render(<MinutesSection />);
    fireEvent.change(await screen.findByLabelText(/transcript retention/i), { target: { value: "5" } });

    const repair = screen.getByRole("button", { name: /repair retention and re-consent/i }) as HTMLButtonElement;
    expect(repair.disabled).toBe(true);
    fireEvent.click(repair);
    expect(minutes.set).not.toHaveBeenCalled();
  });
});
