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

### GPS

![alt text](image-1.png)

![alt text](image-2.png)

### Trusts

![alt text](image-3.png)

![alt text](image-4.png)

## RCS

### RCS Business Messaging

The universally accepted standard for the displayed name of an RBM agent is 25 characters.

#### Rule Set

NHSNoReply is required prefix for all agents that will not be monitoring the reply from participants (approximately 90% of current SMS traffic from the NHS fits this description).

If the agent is designed to be 2 way, NHS may be substituted instead of NHSNoReply.

##### Rule Flow

```mermaid

flowchart TD;
  questionOne[Request for RBM]
  decisonOne{Do you have a valid ODS code?}
  questionOne-->decisonOne
  ruleOne[Allowed NHSNoReply **#91;OSDCode#93;**]
  decisonOne--yes-->ruleOne
  questionTwo{Is primary role R0177 with additional role of R076?}
  ruleOne--Want a more specific sender id-->questionTwo
  ruleTwo[Allowed NHSNoReply GP **#91;OSDCode#93;**]
  questionTwo--yes-->ruleTwo
```

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
#### Alternative
Prefix: 
“NHSNoReply “
	{11 Characters}

Suffix:
“ [ODSCode]” 
	{4-7 Characters}

Naming:
7-10 Characters remaining
General Practice:
	{7 Characters remaining}
** Opt 1: “GP”
		{2 Characters}
* Opt 2: [Custom name set by Org]
		{0-7 Characters}

| Example type | Example RBM Agent Display Name |
|:---|:--- |
|RBM Agent Opt1 |NHSNoReply GP Y01008 |
|RBM Agent Opt2 |NHSNoReply BMG Y01008 |
|SMS Sender ID (Fallback) |NHSgpY01008 |
|NHS App |NHSNoReply GP Bay Medical Group (Y01008) |
|3rd Party App |NHSNoReply GP Bay Medical Group (Y01008) |

Trusts excl. Ambulance:
10 Characters remaining
* Opt 1: [Abbreviation of initials]
		{2-10 Characters}
* Opt 2: [Custom name set by Org]
		{0-10 Characters}

Ambulance Services:
10 Characters remaining
* Opt 1: [Abbreviation of initials]
		{3-7 Characters}
* Opt 2: [Custom name set by Org]
		{0-10 Characters}


| Ambulance Trust Name | Website | Phone | Used Abbreviation |  Suggested RBM Agent Display Name | Character Count |
|:---|:--- |:--- |:--- |:--- |:--- |
| North East Ambulance Service NHS Foundation Trust | https://www.neas.nhs.uk | 0191 430 2000 | NEAS | NHSNoReply NEAS RX6 | 19 |
| Yorkshire Ambulance Service NHS Trust | https://www.yas.nhs.uk | 0330 678 4100 | YAS | NHSNoReply YAS RX8 | 18 |
| North West Ambulance Service NHS Trust | https://www.nwas.nhs.uk | 0345 112 0999 | NWAS | NHSNoReply NWAS RX7 | 19 |
| West Midlands Ambulance Service University NHS Foundation Trust | https://www.wmas.nhs.uk | 01384 215 555 | WMAS | NHSNoReply WMAS RYA | 19 |
| East Midlands Ambulance Service NHS Trust | https://www.emas.nhs.uk | 0115 919 3399 | EMAS | NHSNoReply EMAS RX9 | 19 |
| South Western Ambulance Service NHS Foundation Trust | https://www.swast.nhs.uk | 0300 369 0130 | SWAST | NHSNoReply SWAST RYF | 20 |
| South Central Ambulance Service NHS Foundation Trust | https://www.scas.nhs.uk | 01869 365 000 | SCAS | NHSNoReply SCAS RYE | 19 |
| South East Coast Ambulance Service NHS Foundation Trust | https://www.secamb.nhs.uk | 00300 123 9242 | SECAMB | NHSNoReply SECAMB RYD | 21 |
| London Ambulance Service NHS Trust | https://www.londonambulance.nhs.uk | 020 7783 2000 | LONDAMB | NHSNoReply LONDAMB RRU | 22 |
| East of England Ambulance Service NHS Trust  | https://www.eastamb.nhs.uk | 0345 601 3733 | EASTAMB | NHSNoReply EASTAMB RYC | 22 |
| Welsh Ambulance Services University NHS Trust | https://www.ambulance.nhs.wales |  | WAST | NHSNoReply WAST RT4 | 19 |


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


## Logos and Hero Images
#### Logo
Organisational logo (224 x 224 px; <50 KB)
#### General Practice
Use the org logo, if none is available, use the standard NHS logo 

#### Trusts excl. Ambulance:
Use the org logo, if none is available, use the standard NHS logo 

#### Ambulance Services:
Use the org logo, if none is available, use the standard NHS logo 

#### Hero image
Contains NHS logo (top right) and complies with NHS identity guidelines (1440 x 448 px; 45:14 aspect ratio; <200KB)
#### General Practice
Use the primary care logo (https://www.england.nhs.uk/nhsidentity/identity-guidelines/primary-care-logo/)

#### Trusts excl. Ambulance:
Full organisational Logo, name and descriptor (following NHS identity guidelines)

#### Ambulance Services:
Full organisational Logo, name and descriptor (following NHS identity guidelines)


## NHS NO Reply

Whilst no reply functionality is developed to disable user ability to respond to a message, the adoption of RCS will require 'reply handling'.

- Requirement for NHSNoReply as the prefix - as RCS is only 2 way, and won't show "Not deleivered". It will show delivered, even if an auto reply from the agent comes back saying "This isn't monitored, phone agent on, phone 111, or phone our or 999 etc".
- Suggestion from some clinicians: "**Auto Reply**: Apologies, but we cannot see your reply. If needed, please contact us using your usual method."
- Must include reference to No Reply within Agent description.


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
- General Practice
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
https://www.england.nhs.uk/nhsidentity/identity-guidelines/organisational-logos/
https://www.england.nhs.uk/nhsidentity/identity-guidelines/primary-care-logo/

## Review

Proposal to review 6 months after version 1 released.
