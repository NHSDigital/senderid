# Sender Identification: 0.1.0-alpha

Designing a standard to support digital messaging sender identifications for NHS Organisations.

## Version History

To follow [semantic versioning v2](https://semver.org/).

### 0.1.0-alpha 2025-03-21

Initial Draft.

## Aims

TODO: Complete aims

## Contributors

TODO: Complete aims

## RCS RBM

### RCS Business Messaging

#### Display Name

"NHS [Short Org Name] [Agent Name]" - Max length 30 characters

[Short Org Name] - Max 16 characters - defined by this standard

[Agent Name] - Max 10 characters - available for organization to set

#### Description

"NHS [Full Org Name] ([ODS Code]) [Agent Name] [Agent Description]"

[Full Org Name] - Full org name from ODS

[ODS Code] - Org ODS code

[Agent Description] - available for organization to set.

## Short Org Name

Standard rule for generating a short name from the full ODS organization name.

To consider:

- Standard abbreviations
- Organisation types
- Spacing and punctuation

To author:

- algorithm to generate the short name from a given string
- version releases of full output for all ODS codes - giving a referenceable artifact that can be used

## Defined list

[ODS List](ods.yml)

[RBM Organisation list](senderids.yml)

## Glossary

Terms

- RCS
- RBM

## Resources

[developers.google.com - RCS Business Messaging - Edit agent information](https://developers.google.com/business-communications/rcs-business-messaging/guides/build/agents/edit-agent-information)

## Review

Proposal to review 6 months after version 1 released.