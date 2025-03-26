# Sender Identification: 0.1.0-alpha

Designing a standard to support digital messaging sender identifications for NHS Organisations.

## Version History

To follow [semantic versioning v2](https://semver.org/).

### 0.1.0-alpha 2025-03-21

Initial Draft.

## Aims

- provide a standard to aid uk mobile network operators complete RBM agent identity verification
- provide guidance for when to apply the NHS rule checks
- provide a standardised algorithm for generating sender ids for each NHS organisation
- provide a logic that can be applied to naming conventions of other communication channels that will complement RBM

TODO: Complete aims

## Contributors

## Name Analysis

![alt text](image-1.png)

![alt text](image-2.png)

TODO: Complete aims

## RCS

### RCS Business Messaging

The universally accepted standard for the displayed name of an RBM agent is 25 characters.

#### Rule Set

NHSNoReply is required prefix for all agents that will not be monitoring the reply from participants (approximately 90% of current SMS traffic from the NHS fits this description).

If the agent is designed to be 2 way, NHS may be substituted instead of NHSNoReply.

##### Rule 1

IF
  [ODS: ORG ID] IS VALID AND [NAME] == [Supplied Organisation Name]
THEN
  ALLOW NHSNoReply [ODSCode]

##### Rule 2

IF
  RULE 1 == TRUE
AND
  [ODS: PRIMARY ROLE ID] == R01777
AND
  [ODS: ROLE IDS] CONTAINS R076
THEN
  ALLOW NHSNoReply GP [ODSCode]


- https://developers.google.com/business-communications/rcs-business-messaging 
- https://sinch.com/apis/messaging/rcs/
- https://cpaas.webex.com/business-messaging/rcs-business-messaging
- https://business.bt.com/insights/what-is-rich-business-messaging/

#### Display Name

##### Version One

This would represent a specific agent for the organisation, eg NHS Martin Trust Pharmacy

"NHS [Short Org Name] [Agent Name]" - Max length 25 characters

[Short Org Name] - Max 12 characters - defined by this standard

[Agent Name] - Max 8 characters - available for organisation to set

##### Version Two

This would represent a general agent for the organisation, eg NHS Martin Hospital Trust

"NHS [Medium Org Name]" - Max length 25 characters

[Medium Org Name] - Max 21 characters - defined by this standard

#### Description

"NHS [Full Org Name] ([ODS Code]) [Agent Name] [Agent Description]"

[Full Org Name] - Full org name from ODS

[ODS Code] - Org ODS code

[Agent Description] - available for organisation to set, must contain wording referencing No Reply for NoReply agents. If multi site organisation recommended referencing each site.

#### Phone

Must provide 'the' general enquiries phone number for the organisation

#### Website

Must provide website link to the organisation's web homepage


#### Logos and Hero Images

- hero image - contains NHS logo (top right) and complies with NHS identity guidelines
- logo - organisational logo (224 x 224)

Sizes - resolutions?

TODO: Define logo and image requirements

#### NHS NO Reply

- Requirement for NHSNoReply as the prefix - as RCS is only 2 way, and won't show "Not deleivered". It will show delivered, even if an auto reply from the agent comes back saying "This isn't monitored, phone agent on, phone 111, or phone our or 999 etc".
- Suggestion from some clinicians: "**Auto Reply**: Apologies, but we cannot see your reply. If needed, please contact us using your usual method."
- 

## Short Org Name

Standard rule for generating a short name from the full ODS organization name.

To consider:

- Standard abbreviations
  - geographic eg London -> ldn
  - common words eg general practise -> gp
  - conjuctions eg and &
- Organisation types
- Spacing and punctuation

Additionally:

- NHS Orgs vs Orgs that provide services on behalf of NHS

To author:

- algorithm to generate the short name from a given string
- version releases of full output for all ODS codes - giving a referenceable artifact that can be used

### Agent Name

Initially Agent name can be chosen from:

- Pharmacy
- Outpatient
- Date? (Appointment is 11?)
- Survey
- Feedback
- 

Relax this list in future? Allow ad hoc?

## Defined list

[ODS List](ods.yml)

[RBM Organisation list](senderids.yml)

## Trigger / Protected characteristics

Always require if display name or description containing:

- NHS
- An NHS ODS Code

### Secondary triggers

- Hospital
- General Practise
- Pharmacy
- Doctor
- Ambulance

Exclude if:

- If a registered hospital / doctor / medical org, that is private, ie not NHS.

## Glossary

Terms

- RCS
- RBM

## Resources

[developers.google.com - RCS Business Messaging - Edit agent information](https://developers.google.com/business-communications/rcs-business-messaging/guides/build/agents/edit-agent-information)

## Review

Proposal to review 6 months after version 1 released.
