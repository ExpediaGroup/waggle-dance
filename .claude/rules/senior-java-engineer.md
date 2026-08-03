# Java AGENTS.md Guidelines
<!-- guideline-version: 1.0 — increment when modifying; /agents-md update uses this to detect stale project rule files -->

Java-specific patterns for JVM projects. Use with [jvm.md](jvm.md) for build, dependency management, and shared practices.

---

## Language Version & Style

- **Java 17 minimum, 25 preferred** (17, 21, and 25 are all LTS). New projects should target 25; existing apps on 17 are supported.
- Follow [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- Use **Spotless** with Google Java Format (configured in `pom.xml`); run `mvn spotless:apply` / `mvn spotless:check`
- Naming: `PascalCase` classes, `camelCase` methods/variables, `SCREAMING_SNAKE_CASE` constants, `lowercase.dotted` packages

---

## Modern Java Features

### Records for data classes
```java
public record User(String id, String name, String email) {
  public User {
    Objects.requireNonNull(id, "id cannot be null");
    Objects.requireNonNull(email, "email cannot be null");
  }
}
```

### Sealed classes for type hierarchies
```java
public sealed interface Result<T> permits Result.Success, Result.Failure {
  record Success<T>(T value) implements Result<T> {}
  record Failure<T>(String error) implements Result<T> {}
}
```

### Pattern matching (Java 21+)
```java
String message = switch (result) {
  case Success(var value) -> "Got: " + value;
  case Failure(var error) -> "Error: " + error;
};
```

### Text blocks (Java 15+)
```java
String query = """
    SELECT id, name
    FROM users
    WHERE active = true
    """;
```

---

## Functional Java

- Prefer **immutable collections**: `List.of()`, `Set.of()`, `Map.of()`, `List.copyOf()`
- Use `final` on local variables and fields to signal immutability intent
- Use **Streams** for collection transformations instead of imperative loops:

```java
var activeUserNames = users.stream()
    .filter(User::isActive)
    .map(User::name)
    .sorted()
    .toList();
```

- Use **method references** over lambdas where the intent is clearer: `User::name` not `u -> u.name()`
- Chain **Optional** operations rather than null-checking:

```java
// ❌ Avoid
String name = null;
if (user != null && user.address() != null) {
  name = user.address().city();
}

// ✅ Prefer
String name = Optional.ofNullable(user)
    .map(User::address)
    .map(Address::city)
    .orElse("unknown");
```

---

## Architecture

- Prefer **composition over inheritance**; use constructor injection for dependencies
- Keep **domain logic pure and framework-agnostic** (hexagonal/ports-and-adapters)
- Keep framework code (Spring, etc.) at the edges
- Maximum method length: ~40 lines; maximum class length: ~500 lines
- Use `@NonNull` / `@Nullable` annotations at API boundaries; avoid returning `null` from public methods — use `Optional<T>` instead

---

## Error Handling

Prefer unchecked exceptions for domain errors; use `Optional<T>` as a return type when absence is a normal outcome.

```java
// Domain exceptions — unchecked
public class UserNotFoundException extends RuntimeException {
  public UserNotFoundException(String userId) {
    super("User not found: " + userId);
  }
}

// Optional for normal absence
public Optional<User> findUser(String id) {
  return repository.findById(id);
}

// Sealed Result type for operations that can fail with detail
public sealed interface Result<T> permits Result.Success, Result.Failure {
  record Success<T>(T value) implements Result<T> {}
  record Failure<T>(String error, ErrorCode code) implements Result<T> {}
}
```

- Use checked exceptions only when the caller **must** handle the failure and recovery is meaningful
- Do not use exceptions for control flow
- Always include context in exception messages (`"User not found: userId=" + id`)

---

## Concurrency

- For I/O-bound work on **Java 21+**, prefer **virtual threads** over thread pools:

```java
// Spring Boot 3.2+ — enable in application.yml:
// spring.threads.virtual.enabled: true

// Or explicitly:
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
  executor.submit(() -> fetchFromDatabase(id));
}
```

- Use **`CompletableFuture`** for async pipelines on Java 17:

```java
CompletableFuture.supplyAsync(() -> repository.findById(id))
    .thenApply(User::toDto)
    .exceptionally(e -> UserDto.empty());
```

- Avoid `synchronized` on broad scopes; prefer `java.util.concurrent` types (`ConcurrentHashMap`, `AtomicReference`)

---

## Logging

Use **SLF4J** as the facade (`LoggerFactory.getLogger`) with **Log4j2** as the implementation. For Spring Boot projects, exclude `spring-boot-starter-logging` and use `spring-boot-starter-log4j2`.

### Basic pattern
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserService {
  private static final Logger log = LoggerFactory.getLogger(UserService.class);

  public User getUser(String id) {
    log.debug("Fetching user: userId={}", id);
    try {
      var user = repository.findById(id);
      log.info("User found: userId={}", user.getId());
      return user;
    } catch (Exception e) {
      log.error("Failed to fetch user: userId={}", id, e);
      throw new UserNotFoundException(id);
    }
  }
}
```

### MDC for request correlation
Use MDC to attach context that should appear on every log line within a request scope:

```java
MDC.put("requestId", requestId);
MDC.put("userId", userId);
try {
  processRequest();
} finally {
  MDC.clear();
}
```

Log levels: `DEBUG` for diagnostic traces, `INFO` for lifecycle events, `WARN` for recoverable failures, `ERROR` for unhandled exceptions. Use parameterised logging (`{}`) — never string concatenation.

---

## Testing

### Frameworks
- **JUnit 5** for test structure
- **Mockito** for mocking
- **AssertJ** for fluent assertions
- **Testcontainers** for integration tests (databases, external services)

### Test layout
See [jvm.md](jvm.md) for the standard test layout and integration test naming conventions.

### Test structure
```java
@ExtendWith(MockitoExtension.class)
public class UserServiceTest {

  @Mock
  private UserRepository repository;

  @Mock
  private NotificationService notificationService;

  @Test
  public void getUser_whenFound_returnsUser() {
    // Given
    when(repository.findById("123")).thenReturn(Optional.of(mockUser));
    var service = new UserService(repository, notificationService);

    // When
    var user = service.getUser("123");

    // Then
    assertThat(user)
        .isPresent()
        .get()
        .extracting(User::id, User::name)
        .containsExactly("123", "John Doe");
    verify(repository).findById("123");
  }

  @Test
  public void getUser_whenNotFound_throwsException() {
    // Given
    when(repository.findById("999")).thenReturn(Optional.empty());
    var service = new UserService(repository, notificationService);

    // When/Then
    assertThatThrownBy(() -> service.getUser("999"))
        .isInstanceOf(UserNotFoundException.class);
  }
}
```

### AssertJ patterns
- **Soft assertions** — report all failures at once:
  ```java
  assertSoftly(softly -> {
    softly.assertThat(user.id()).isEqualTo("123");
    softly.assertThat(user.name()).isEqualTo("John Doe");
  });
  ```
- **Extracting** for collection checks: `.extracting(User::id).containsExactly("1", "2")`
- Prefer concrete inputs; avoid randomness and time-dependent logic in tests

### Mockito
- Prefer `@ExtendWith(MockitoExtension.class)` with `@Mock` fields over inline `mock()` calls
- Use `when().thenReturn()` for stubbing, `verify()` for interaction checks
- Construct the class under test in each test method, passing `@Mock` fields via constructor injection
- Prefer specific matchers over `any()` where intent matters

---

## Anti-patterns

- ❌ Returning `null` from public methods — use `Optional<T>`
- ❌ Catching `Exception` or `Throwable` generically
- ❌ Catching, logging, and re-throwing — pick one: either handle it, or let it propagate (log at the boundary where you handle it)
- ❌ Checked exceptions for control flow
- ❌ Mutable DTOs with getters/setters — use records or immutable classes
- ❌ Raw types (`List` instead of `List<String>`)
- ❌ String concatenation in loops — use `StringBuilder` or streams
- ❌ `new HashMap<>()` for read-only maps — use `Map.of()`
- ❌ `static` mutable state
- ❌ Deep inheritance hierarchies — prefer composition
- ❌ `System.out.println` or `java.util.logging` — use SLF4J
- ❌ Overusing `@SuppressWarnings("unchecked")` — fix the root cause
- ❌ Overusing inline `mock()` calls — prefer `@Mock` fields with `@ExtendWith(MockitoExtension.class)`
