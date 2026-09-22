import React from "react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../../contributions", () => ({ registerTab: vi.fn() }));

import { ConfigForm, MODEL_FIELDS } from "../settings";

afterEach(cleanup);

const fields = [
  { key: "url", label: "Service URL" },
  { key: "token", label: "Token", secret: true },
];

describe("ConfigForm secret write semantics", () => {
  it("always renders a masked/unchanged secret as password and omits it from unrelated updates", async () => {
    const save = vi.fn(async (update: Record<string, string>) => ({
      url: update.url ?? "http://old-stt",
      token: "********cret",
    }));
    render(<ConfigForm
      fields={fields}
      load={async () => ({ url: "http://old-stt", token: "********cret" })}
      save={save}
    />);

    const token = await screen.findByLabelText("Token") as HTMLInputElement;
    expect(token.type).toBe("password");
    expect(token.value).toBe("********cret");

    fireEvent.change(screen.getByLabelText("Service URL"), { target: { value: "http://old-stt/v1" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ url: "http://old-stt/v1" }));
  });

  it("sends an explicit empty string when the admin clears a stored secret", async () => {
    const save = vi.fn(async () => ({ url: "http://old-stt" }));
    render(<ConfigForm
      fields={fields}
      load={async () => ({ url: "http://old-stt", token: "********cret" })}
      save={save}
    />);

    const token = await screen.findByLabelText("Token") as HTMLInputElement;
    fireEvent.change(token, { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ token: "" }));
  });

  it("never sends a masked placeholder as a replacement credential", async () => {
    const save = vi.fn(async () => ({}));
    render(<ConfigForm
      fields={fields}
      load={async () => ({ url: "http://old-stt", token: "********cret" })}
      save={save}
    />);

    const token = await screen.findByLabelText("Token") as HTMLInputElement;
    fireEvent.change(token, { target: { value: "********cret-extra" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect((await screen.findByRole("alert")).textContent).toMatch(/replace the masked token completely/i);
    expect(save).not.toHaveBeenCalled();
  });

  it.each([
    ["Deployment default", ""],
    ["Claude subscription (deployment credentials)", "subscription"],
  ])("clears hidden custom provider state when switching to %s", async (label, mode) => {
    const save = vi.fn(async (update: Record<string, string>) => ({
      mode,
      model: "user-model",
      ...update,
    }));
    render(<ConfigForm
      fields={MODEL_FIELDS}
      load={async () => ({
        mode: "custom",
        base_url: "https://user-model.example/v1",
        api_key: "********-key",
        model: "user-model",
      })}
      save={save}
    />);

    fireEvent.change(await screen.findByLabelText("Provider"), { target: { value: mode } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(save).toHaveBeenCalledWith({
      mode,
      base_url: "",
      api_key: "",
    }));
  });

  it("shows a blocked personal endpoint instead of implying platform fallback", async () => {
    render(<ConfigForm
      fields={MODEL_FIELDS}
      load={async () => ({
        mode: "custom",
        base_url: "https://revoked-model.example/v1",
        config_status: "blocked",
        validation_error: "Personal model endpoint is no longer operator-approved.",
      })}
      save={async () => ({})}
    />);

    expect((await screen.findByRole("alert")).textContent).toContain(
      "Personal model endpoint is no longer operator-approved.",
    );
  });
});
