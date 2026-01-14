# sms-dashboard

## SSL troubleshooting

If you see SSL errors when sending SMS, the Africa's Talking SDK uses HTTPS under the
hood and relies on the host's CA certificates. You can optionally configure:

* `AT_CA_BUNDLE` - path to a custom CA bundle file to trust.
* `AT_SSL_VERIFY` - set to `false` to disable TLS verification for outbound
  requests (not recommended).
