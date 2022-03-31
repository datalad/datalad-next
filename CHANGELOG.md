# 0.1.0 (2022-03-31) --  Credentials, please!

#### 💫 Enhancements and new features
- A new credential management system is introduced that enables storage and query of credentials with any number of properties associated with a secret. These properties are stored as regular configuration items, following the scheme `datalad.credential.<name>.<property>`. The special property `secret` lives in a keystore, but can be overriden using the normal configuration mechanisms. The new system continues to support the previous credential storage setup. Fixes [#6519](https://github.com/datalad/datalad/issues/6519) ([@mih](https://github.com/mih))
- A new `credentials` command enables query, modification and storage of credentials. Legacy credentials are also supported, but may require the specification of a `type`, such as (`token`, or `user_password`) to be discoverable. Fixes [#396](https://github.com/datalad/datalad/issues/396) ([@mih](https://github.com/mih))
- Two new configuration settings enable controlling how the interactive entry of credential secrets is conducted for the new credential manager: `datalad.credentials.repeat-secret-entry` can be used to turn off the default double-entry of secrets, and `datalad.credentials.hidden-secret-entry` can turn off the default hidden entry of secrets. Fixes [#2721](https://github.com/datalad/datalad/issues/2721) ([@mih](https://github.com/mih))


#### Authors: 1

- Michael Hanke ([@mih](https://github.com/mih))

---
