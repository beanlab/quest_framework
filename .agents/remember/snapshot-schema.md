# Snapshot Action Schemas

## Status

Deferred feature record. Action-schema work should begin only after the core resource and subscription refactor described in [resource-stream-revisit.md](resource-stream-revisit.md) is complete. Nothing in this document is part of the current snapshot protocol unless it is explicitly adopted later.

## Context

Quest exposes a workflow-controlled resource surface to observers. Each resource snapshot currently identifies the resource by type, name, and identity, includes values exposed through `@external_value`, and lists methods exposed through `@external_action`.

The minimal protocol publishes action names only:

```json
{
  "type": "queue",
  "name": "response",
  "identity": "user-1",
  "state": {},
  "actions": ["put"]
}
```

An observer that already understands the resource type can provide a protective, type-specific interface. A completely generic observer, however, cannot determine an action's expected arguments or result from its name alone.

## Motivation

Generic action schemas could make the resource surface self-describing enough to support:

- Swagger-like workflow inspection and testing interfaces;
- generated forms or debugging tools;
- agents that discover and invoke workflow actions directly;
- generic client tooling that does not have a prebuilt interface for every resource type;
- adapters to tool protocols such as OpenAI function calling or MCP.

This metadata would usually not be needed by an ordinary application UI. Its value is in generic observation and interaction tooling.

## Goal

The current protocol remains unchanged: `actions` is a list of names, and Quest does not currently generate or transport parameter schemas, return schemas, or action descriptions.

A later feature may derive portable action metadata from the decorated Python method without requiring authors to describe the same interface twice. A future snapshot could provide, for each external action:

- the action name;
- a JSON Schema for its parameters;
- a JSON Schema for its return value;
- the method docstring as a description when available.

Quest should publish provider-neutral metadata. Consumers, rather than Quest, should wrap it in OpenAI-, MCP-, Swagger-, or other provider-specific records.

A shape considered during design was:

```json
{
  "actions": {
    "put": {
      "description": "Add an item to the queue.",
      "parameters": {
        "type": "object",
        "properties": {
          "item": {}
        },
        "required": ["item"],
        "additionalProperties": false
      },
      "returns": {}
    }
  }
}
```

This is exploratory, not a settled future protocol.

## Design considerations

### Invocation semantics

An object-shaped parameter schema naturally implies invocation with named arguments:

```python
method(**arguments)
```

Ordinary positional-or-keyword and keyword-only parameters work with that model. Positional-only parameters do not. Literal `*args` and `**kwargs` require deliberate representation and argument reconstruction rules; merely generating a schema is not enough to preserve Python calling semantics.

A future implementation should either constrain external action signatures or retain the original `inspect.Signature` and convert validated object input into positional and keyword arguments before invocation.

### Missing and explicit annotations

The discussion favored an unconstrained JSON Schema, `{}`, for a missing annotation. It is a valid schema accepting any value and allows consumers to process every entry uniformly.

A missing return annotation must remain distinguishable from an explicit `-> None`, which should produce a null-value schema such as:

```json
{"type": "null"}
```

These choices should be reconfirmed when implementation begins.

### Required parameters and defaults

A likely rule is:

- a parameter is required only when it has no Python default;
- `Optional[T]` permits `null` but does not by itself make the argument omittable;
- a serializable Python default may be included as JSON Schema `default`;
- unsupported defaults need explicit failure or omission behavior.

All published metadata and defaults must be supported by Quest's configured serializer.

### Supported Python types

Python's type system is broader than JSON Schema. The implementation must define supported behavior for at least:

- primitives and `Any`;
- unions and nullable types;
- lists, tuples, and string-keyed mappings;
- `Literal` and `Enum`;
- `TypedDict` and dataclasses;
- forward references and `Annotated` metadata;
- custom classes and configured serializers;
- generic types, `TypeVar`, `ParamSpec`, and callable types.

Unsupported annotations may fail schema construction, degrade to `{}`, or require an author-provided schema. That policy remains open. Because schema generation occurs on the observer side and is not recorded or replayed, a schema bug should not alter workflow history.

### Snapshot failure scope

Schema generation would become part of snapshot construction. A generation error should therefore follow the snapshot-failure policy: fail only the affected one-shot read or subscription, without failing or changing the workflow, historian, or other subscriptions.

### Protocol evolution

Changing `actions` from a list of names to a metadata mapping is a protocol change. This feature should be introduced deliberately after the core refactor is stable. The project remains experimental, so compatibility can be managed explicitly rather than by prematurely carrying both representations.

## Library investigation: `toolschema`

`toolschema` was investigated as a possible implementation dependency. Version 1.0.1 was small:

- approximately 29 KB wheel;
- approximately 72 KB installed;
- approximately 1,646 lines of Python source;
- MIT licensed;
- no transitive runtime dependency on Quest's supported Python 3.11+ versions.

Its core API closely matches the feature:

```python
definition = toolschema.schema(method)

definition.description
definition.parameters
definition.output
```

It supports JSON Schema 2020-12, docstrings, defaults and required parameters, common typing constructs, dataclasses, `TypedDict`, optional Pydantic models, and return annotations.

The investigated version was not a drop-in fit:

- `*args` and `**kwargs` were silently skipped;
- positional-only parameters were emitted as ordinary object properties even though keyword invocation cannot call them;
- a missing return annotation and explicit `-> None` were both represented as no output schema;
- unsupported annotations raised `TypeError`;
- defaults were inserted without Quest-specific serializer checks;
- schema generation did not solve action invocation or argument reconstruction.

The package was classified Beta and had a small community footprint. Its dependency cost was negligible, but semantic adaptation would still be required. Reevaluate its current release when this feature is implemented rather than adding it during the core refactor.

References:

- <https://pypi.org/project/toolschema/>
- <https://toolschema.readthedocs.io/>
- <https://github.com/false200/toolschema>

Pydantic and OpenAI Agents' function-schema implementation are other useful references. OpenAI Agents notably retains `inspect.Signature` and reconstructs positional and keyword arguments after validating an object-shaped input, but depending on an entire agent framework would be inappropriate for Quest's core.

## Implementation sequencing

1. Complete and stabilize the authoritative-latest resource surface and non-backpressuring subscription refactor.
2. Gain experience with the minimal action-name protocol.
3. Reconfirm concrete consumers for schemas and the exact protocol shape.
4. Reevaluate available schema-generation libraries.
5. Specify signature constraints, type support, failures, invocation reconstruction, and serialization.
6. Implement schema generation and generic-tool tests as a separate feature.

## Open questions

- Is an action metadata mapping the final protocol shape?
- Which JSON Schema draft should Quest promise?
- Which Python annotations are supported?
- How are unsupported annotations and unserializable defaults handled?
- How are positional-only, `*args`, and `**kwargs` represented and invoked?
- Should Quest validate arguments against the schema, or is the schema descriptive only?
- How can authors override or extend generated schemas?
- Are return schemas always present, or omitted when information is unavailable?
- Should schemas be cached per resource class or generated for each snapshot?
- Which library, if any, should provide type-to-schema conversion?

## Verification considerations

Future tests should cover:

- parameter and return schemas for common annotations;
- docstring descriptions;
- required and defaulted parameters;
- missing annotations versus explicit `None`;
- unions, nullable values, collections, enums, dataclasses, and `TypedDict`;
- forward references and `Annotated` metadata;
- unsupported annotations and defaults;
- positional-only and variadic signatures;
- argument reconstruction and invocation;
- configured serializer behavior;
- schema-generation failures affecting only the observing read or subscription;
- adapters or sample consumers for generic testing and agent interaction.
