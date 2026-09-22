# auth/login

`POST {email}` — local debug-only login. The route is default off and accepts only an exact,
case-insensitive member of `VEXA_DIRECT_LOGIN_ALLOWED_EMAILS` while both the declared Terminal origin
and effective host listener/publication are loopback. It cannot create the first administrator.

For an allowed existing/provisionable user, it asks admin-api to find-or-create the account, mints an
APIToken (scopes `bot`, `tx`, `browser`), and sets the httpOnly `vexa-token` cookie. No email or
password is sent. Use OAuth in a hosted or otherwise network-facing deployment.
