/** Browser-edge payload limits kept outside route modules (Next route exports are schema-checked). */
export const MAX_PROXY_REQUEST_BYTES = 2 * 1024 * 1024;
export const MAX_PROXY_RESPONSE_BYTES = 16 * 1024 * 1024;
export const MAX_PROXY_ERROR_RESPONSE_BYTES = 8 * 1024;
export const MAX_CHAT_REQUEST_BYTES = 512 * 1024;
export const MAX_SETTINGS_REQUEST_BYTES = 64 * 1024;
export const MAX_SETTINGS_RESPONSE_BYTES = 256 * 1024;
export const MAX_AUTH_REQUEST_BYTES = 64 * 1024;
export const MAX_ADMIN_PROXY_RESPONSE_BYTES = 2 * 1024 * 1024;
export const MAX_WORKSPACE_REQUEST_BYTES = 32 * 1024 * 1024;
export const MAX_WORKSPACE_RESPONSE_BYTES = 32 * 1024 * 1024;
