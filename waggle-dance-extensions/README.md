## Waggle Dance Extensions

### Overview
This project consists of extensions to Waggle Dance. By design these extension should add functionality that users may optionally configure to use. 
The main Waggle Dance project works without these extensions.

### Rate Limiting

Extension that allows for Rate Limiting calls to Waggle Dance.

To enable and configure see the following table, you can add these properties to the waggle-dance-server.yml:

  | Property | Required | Description |
  | --- | --- | --- |
  | waggledance.extensions.ratelimit.enabled | no | Whether the rate limiting extension is enabled. Default is `false` |
  | waggledance.extensions.ratelimit.storage | yes (if `enabled: true`) | The storage backend for the rate limiter, possible values `MEMORY` or `REDIS` |
  | waggledance.extensions.ratelimit.capacity | no | The capacity of the bucket. Default `2000` |
  | waggledance.extensions.ratelimit.refillType | no | The refill type, possible values `GREEDY` or `INTERVALLY`. Default is `GREEDY` |
  | waggledance.extensions.ratelimit.tokensPerMinute | no | The number of tokens to add to the bucket per minute. Default `1000` |
  | waggledance.extensions.ratelimit.reddison.embedded.config | yes (if `storage: REDIS`) | The configuration for Redisson client, can be added in a similar way as described [here](https://github.com/redisson/redisson/tree/master/redisson-spring-boot-starter#2-add-settings-into-applicationsettings-file) |

#### InMemory Rate limiting.

Example config:

```
waggledance.extensions.ratelimit.enabled: true
waggledance.extensions.ratelimit.storage: memory
waggledance.extensions.ratelimit.capacity: 2000
waggledance.extensions.ratelimit.tokensPerMinute: 1000
```

#### Redis stored buckets Rate limiting.

Using a Redis backend server is supported by this module, it's up to the user to configure and maintain that infrastructure. 
The next example assumes a Redis Replicated Server is running using SSL and `auth_token` authentication.
Timeouts and retry are set lower than default to impact the Waggle Dance service. 
The maximum latency this solution will add to a request in the following scenarios is: 
1. Redis server down:
  * Latency will be in low ms as `retryAttemps: 0`, the connection will immediately fail.
2. Redis server slow:
  * Latency will be max `timeout: 1000` ms
 
Waggle Dance is configured to allow all requests in case of Rate Limiting Server failures.

Example config using a Redis Replicated Server:

```
waggledance.extensions.ratelimit.enabled: true
waggledance.extensions.ratelimit.storage: memory
waggledance.extensions.ratelimit.capacity: 2000
waggledance.extensions.ratelimit.tokensPerMinute: 1000
waggledance.extensions.ratelimit.reddison.embedded.config: |
  replicatedServersConfig:
    idleConnectionTimeout: 10000
    connectTimeout: 3000
    timeout: 1000
    retryAttempts: 0
    retryInterval: 1500
    password: "<auth_token>"
    nodeAddresses:
    - "rediss://localhost1:62493"
    - "rediss://localhost2:62493"
```

For more configuration options and details please consult: [https://redisson.org/](https://redisson.org/) and [https://bucket4j.com/](https://bucket4j.com/)


## Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2024 Expedia Inc.