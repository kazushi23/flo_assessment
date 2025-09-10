File naming - no spacing
VersionHeader # Unique ID # From # To.csv
Example = nem12 # 0123456789012345 # mda1 # retail1.csv

File can be .zip extension, not only .csv

(a) The MDFF must:
(i) be constructed in a CSV format;
(ii) contain only a single Header (100) record;
(iii) contain a single End (900) record; and
(iv) contain NEM12 or NEM13 formatted data, but not both.
(b) Due to delays with the updating of MSATS, recipients of an MDFF file should be aware that the
information provided in the MDFF file might not align with MSATS at the time of receipt.
(c) The MDP must ensure that all NMI suffixes associated with a NMI for any IntervalDate are included
in the same 100-900 event block.

(b) For 5-minute data:
    (i) The first Interval (1) for a meter programmed to record 5-minute interval metering
    data would relate to the period ending 00:05 of the IntervalDate.
    (ii) The last Interval (288) for a meter programmed to record 5-minute interval metering
    data would relate to the period ending 00:00 of the IntervalDate+1.
(c) For 15-minute data:
    (i) The first Interval (1) for a meter programmed to record 15-minute interval metering data
    would relate to the period ending 00:15 of the IntervalDate.
    (ii) The last Interval (96) for a meter programmed to record 15-minute interval metering data
    would relate to the period ending 00:00 of the IntervalDate+1.
(d) For 30-minute data:
    (i) The first Interval (1) for a meter programmed to record 30-minute interval metering data
    would relate to the period ending 00:30 of the IntervalDate.
    (ii) The last Interval (48) for a meter programmed to record 30-minute interval metering data
    would relate to the period ending 00:00 of the IntervalDate+1.

Reason Code
The following rules apply to the use of reason codes:
(a) The MDP must apply the ReasonCode that most accurately reflects the reason for supplying the
code or based on the hierarchical structure agreed with the FRMP.
(b) A ReasonCode must be provided for all Intervals and consumption values where the QualityFlag ‘S’
(substituted metering data) or ‘F‘ (final substituted metering data).
(c) A ReasonCode must be provided for Actual Meter Readings (QualityFlag ‘A’) for all Intervals where
the meter has recorded a power outage (reason code 79), time reset (reason code 89), or tamper
(reason code 61).
(d) Other ReasonCodes may be provided where the QualityFlag value is ‘A’.
(e) Multiple Interval event records (400 record) are allowed for each interval metering data record
(300 record) where more than one ReasonCode is applicable to the day’s Meter Readings.
(f) Only one QualityMethod and one ReasonCode can be applied to an Interval.
(g) Where the QualityMethod is ‘V’ (variable data) a ReasonCode is not to be provided.
(h) The complete list of available reason codes, with accompanying descriptions, and obsolete reason
codes are detailed in Appendix E and Appendix F. Obsolete reason codes are provided to support
the provision of Historical Data only.
(i) Quality flag meanings and relationships with other fields are detailed in Appendix C.

Mandatory and required data
The key to the initials used in the Field Requirement column of all Record data tables in sections 4 and 5 is
as follows:
Key M = Mandatory (must be provided in all situations).
R = Required (must be provided if this information is available).
N = Not required (unless specified, can be provided but may be ignored by
the recipient).
Where more than one initial is used in the ‘Field Requirement’ column, the ‘Definitions’ column provides
clarification on the scenarios where each initial applies.

Date formats:
CCYYMMDD 20030501
CCYYMMDDhhmm 200301011534
CCYYMMDDhhmmss 20030101153445

100 - header
200 - NMI data details
300 - interval data
400 - interval event
500 - b2b details
900 - end

100,NEM12,200301011534,MDP1,Retailer1
RecordIndicator,VersionHeader,DateTime,FromParticipant,ToParticipant
RecordIndicator - 100,200 ... 900
VersionHeader - NEM12
DateTime - file creation datetime
FromParticipant - participant id (MDP = metered data provider)
ToParticipant - participant id (MDP/ENM, ENM = entity utilising data)

200,VABD000163,E1Q1,1,E1,N1,METSER123,kWh,30,20040120
Multiple 300-500 records are allowed in a single 200 record
RecordIndicator,NMI,NMIConfiguration,RegisterID,NMISuffix,MDMDataStreamIdentifier,MeterSerialNumber,UOM,IntervalLength,NextScheduledReadDate
RecordIndicator - 200
NMI - NMI for connection point (National Metering Identifier)
NMIConfiguration - string of all NMUSuffixes applicable to the NMI
RegisterID - Interval Meter register identifier
NMISuffix - defined in the NMI Procedure
MDMDataStreamIdentifier - Defined as per the suffix field in the CATS_NMI_DataStream table, e.g. “N1”, “N2”
MeterSerialNumber - The Meter Serial ID of the meter installed at a Site. if meter is replaced, the serial ID of new meter will apply on and from the IntervalDate when the meter is replaced
UOM - unit of measure of data
IntervalLength - time in mins of each interval period: 5,15 or 30
NextScheduledReadDate - date is in NSRD

250,1234567890,1141,01,11,11,METSER66,E,000021.2,20031001103230,A,,,000534.5,20040201100 030,E64,77,,343.5,kWh,20040509, 20040202125010,20040203000130 
RecordIndicator,NMI,NMIConfiguration,RegisterID,NMISuffix,MDMDataStreamIdentifier,MeterSerialNumber,DirectionIndicator,PreviousRegisterRead,PreviousRegisterReadDateTime,PreviousQualityMethod ,PreviousReasonCode,PreviousReasonDescription,CurrentRegisterRead,CurrentRegisterReadDateTime,CurrentQualityMethod,CurrentReasonCode,CurrentReasonDescription,Quantity,UOM,NextScheduledReadDate,UpdateDateTime,MSATSLoadDateTime 
RecordIndicator - 250
NMI - NMI for the connection point. 
NMIConfiguration - String of all applicable NMISuffixes for the NMI.
RegisterID - Accumulation meter register identifier.  Defined the same as the RegisterID field in the 
NMISuffix - As defined in the NMI Procedure -  e.g. “11”, “41”. 
MDMDataStreamIdentifier - Defined as per the suffix field in the CATS_NMI_DataStream table, e.g. “11”, “41”. The value must match the value in MSATS. The field must be provided if the metering data has or would be sent to MDM. 
MeterSerialNumber - Meter Serial ID as per Standing Data for  MSATS. 
DirectionIndicator - A code to indicate whether this register records “Import” or “Export”. Allowed values: ‘I’ = Import to grid, ‘E’ = Export from grid “Import” means that energy normally flows from the connection point to the grid. “Export” means energy normally flows from the grid to the connection point. 
PreviousRegisterRead - Example of values: 1234567.123 or 0012456.123. Values must include any leading zeros and trailing zeros as per the physical dial format. Values must be exclusive of meter multipliers. The ‘previous’ Meter Reading is the earlier of the two Meter Readings provided.  An  Estimate cannot be provided in the PreviousRegisterRead field. 
PreviousRegisterReadDateTime - Actual date/time of the Meter Reading. The date/time the transaction occurred or, for a substitution (quality flag = ‘S’ or ’F’), when the Meter Reading should have occurred. 
PreviousQualityMethod - Data quality & Substitution/Estimation flag for PreviousRegisterRead. Format :In the form QMM, where quality flag (Q) = 1 character and method flag (MM) = 2 character.  
PreviousReasonCode - Reason for Substitute/Estimate or information for PreviousRegisterRead. 
PreviousReasonDescription - Description of ReasonCode for PreviousRegisterRead. Description of ReasonCode for PreviousRegisterRead. 
CurrentRegisterRead - Register read. Example of values: 1234567.123 or 0012456.123. Values must include any leading zeros and trailing zeros as per the physical dial format. Values must be exclusive of meter multipliers. The ‘current’ Meter Reading is the later of the two Meter Readings provided.  It has no specific relationship to the present; for example, it may be in the future if the Meter Reading is an Estimate. 
CurrentRegisterReadDateTime - Actual date/time of the Meter Reading. For Estimates, the date should be equal to or greater than the NextScheduledReadDate, with a time component of 00:00:00 (ie, date(8) + 000000). The date/time the transaction occurred or, for a Substitution (quality flag = ‘S’ or ‘F’), when Meter Reading should have occurred. The time component of the CurrentRegisterReadDateTime should be the actual time of the attempted Meter Reading. If this is not available the value of the time component must be 00:00:01.
CurrentQualityMethod - Data quality & Substitution/Estimation flag for CurrentRegisterRead. Format: In the form QMM, where quality flag (Q) = 1 character and method flag (MM) = 2 character. Allowed values: See quality and Method tables (refer Appendix C & D). If quality flag = “A”, no method flag is required.
CurrentReasonCode - Reason for Substitute/Estimate or information for CurrentRegisterRead. Refer to section 3.3.5 for more details. Allowed values: Refer Appendix E. Not Required where the quality flag = ‘A’ or ‘E’ but can be provided for information.
CurrentReasonDescription - Description of ReasonCode for CurrentRegisterRead. Mandatory where the CurrentReasonCode is ‘0’.
Quantity - The computed quantity, after the application of any multiplier value and taking account of any meter rollover. For energy values (e.g. watt hours or var hours) this is measured between the CurrentRegisterRead and PreviousRegisterRead (CurrentRegisterRead value less PreviousRegisterRead value corrected for the register multiplier). For non-energy (demand) values, it is the CurrentRegisterRead corrected for the register multiplier. A negative value must not be provided 
UOM - Unit of Measure for the Quantity value.
NextScheduledReadDate -This date is the NSRD. This field is not required where the meter will not be read again (e.g. meter removed, NMI abolished, MDP will no longer be the MDP). The NSRD provided in this file is accurate at the time the file is generated (noting this may be subject to change e.g. if route change etc.). MSATS is the database of record, therefore, should there be a discrepancy between the NSRD in this file, MSATS shall prevail.
UpdateDateTime - The latest date/time for the updated CurrentRegisterRead or CurrentQualityMethod. This is the MDP’s version date/time that the metering data was created or changed. This date and time applies to data in this 250 record.
MSATSLoadDateTime - This is the date/time stamp MSATS records when metering data was loaded into MSATS. This date is in the acknowledgement notification sent to the MDP by MSATS

300,20030501,50.1, . . . ,21.5,V,,,20030101153445,20030102023012
RecordIndicator,IntervalDate,IntervalValue1 . . . IntervalValueN,QualityMethod,ReasonCode,ReasonDescription,UpdateDateTime,MSATSLoadDateTime
RecordIndicator - 300
IntervalDate - interval date
IntervalValue1. . . IntervalValueN - The total amount of energy or other measured value for the Interval inclusive of any multiplier or scaling factor. The number of values provided must equal 1440 divided by the IntervalLength. This is a repeating field with individual field values separated by comma delimiters. Allowed value rules: A negative value is not allowed.  The value may contain decimal places. Exponential values are not allowed. 
QualityMethod - Substitution/Estimation flags for all IntervalValues contained in this record.  The QualityMethod applies to all IntervalValues in this record.  Where multiple QualityMethods or ReasonCodes apply to these IntervalValues, a quality flag ‘V’ must be used. Format:  In the form QMM, where quality flag ('Q) = 1 character and method flag (MM) = 2 character. Allowed values: See quality and method tables (Appendix C & D). If quality flag = ’A’ or ’V‘ no method flag is required. 
ReasonCode - Summary of the reasons for Substitute/Estimate or  information for all IntervalValues contained in this record. The ReasonCode applies to all IntervalValues in this record.   Not required if quality flag = ’A’ or ‘E‘, but can be provided for information. The field must not be populated if quality flag = ’V’. 
ReasonDescription - Description of ReasonCode. Mandatory where the ReasonCode is ’0’. 
UpdateDateTime - latest date/time that any updated intervalvalue or qualitymethod for the intervaldate
MSATSLoadDateTime - date/time stamp MSATS records when metering data loaded

400,1,28,S14,32,
THIS IS MANDATORY WHEN QUALITYFLAG == V in 300 or WHEN QUALITYFLAG == A && REASONCODE == 79 || 89 || 61
RecordIndicator,StartInterval,EndInterval,QualityMethod,ReasonCode,ReasonDescription
RecordIndicator - 400
StartInterval - The first Interval number that the ReasonCode/QualityMethod combination applies to. The StartInterval must be less than or equal to the EndInterval. 
EndInterval - The last Interval number that the ReasonCode/QualityMethod combination applies to. 
QualityMethod - Data quality & Substitution/Estimation flag for metering data.  The QualityMethod applies to all IntervalValues in the inclusive range defined by the StartInterval and EndInterval. Format:  In the form QMM, where quality flag (Q) = 1 character and method flag (MM) = 2 character Allowed values: See quality and method tables (refer Appendices C & D). If quality flag = “A” no method required. The quality flag of “V” cannot be used in this record
ReasonCode - Reason for Substitute/Estimate or information.   The ReasonCode applies to all IntervalValues in the inclusive range defined by the StartInterval and EndInterval.  Not required if quality flag = “E” but can be provided for information.
ReasonDescription - Description of ReasonCode. Mandatory where the ReasonCode is “0”. The ReasonDescription applies to all IntervalValues in the inclusive range defined by the StartInterval and EndInterval. 

500,S,RETNSRVCEORD1,20031220154500,001123.5
This record is mandatory where a manual Meter Reading has been performed or attempted.
RecordIndicator,TransCode,RetServiceOrder,ReadDateTime,IndexRead
RecordIndicator - 500
TransCode - Indicates why the recipient is receiving this metering data. Refer Appendix A for a list of allowed values for this field. A value of ‘O’ (i.e. capital letter O) must be used when providing Historical Data and where this information is unavailable. 
RetServiceOrder - The Service Order number associated with the Meter Reading.  
ReadDateTime - Actual date/time of the Meter Reading. The date/time the transaction occurred or, for a Substitution (quality flag = ‘S’ or ‘F’), when the Meter Reading should have occurred. The time component of the ReadDateTime should be the actual time of the attempted Meter Reading.  If this is not available the value of the time component must be 00:00:01. The ReadDateTime is required when providing Historical Data and not required for Estimates.
IndexRead - The total recorded accumulated energy for a Datastream retrieved from a meter’s register at the time of collection.  For type 4A and type 5 metering installations the MDP must provide the IndexRead when collected. Refer section 3.3.4. 

550,N,,A, 
RecordIndicator,PreviousTransCode,PreviousRetServiceOrder,CurrentTransCode, CurrentRetServiceOrder 
RecordIndicator - 550
PreviousTransCode - CurrentRetServiceOrder
PreviousRetServiceOrder - The retailer’s ServiceOrderRequest  number associated with the PreviousRegisterRead reading (where the metering data is directly associated with a ServiceOrderRequest). 
CurrentTransCode - Indicates why the CurrentRegisterRead was collected. Refer Appendix A for a list of allowed values for this field. 
CurrentRetServiceOrder - The retailer’s Service Order number associated with the CurrentRegisterRead reading (where the metering data is directly associated with a B2B service order request). 

900

TRANSACTION CODE FLAGS
A - Alteration - Any action involving the alteration of the metering installation at a Site. This includes a removal of one meter and replacing it with another and all new connections and ‘Add/Alts’ Service Orders.   
C - Meter Reconfiguration - Any action involving the alteration of the metering installation at a Site. This includes a removal of one meter and replacing it with another and all new connections and ‘Add/Alts’ Service Orders.   
G - Re-energisation - ‘Re-energisation’ Service Order. 
D - De-energisation - ‘De-energisation’, including ‘De-energisation for Non-payment’ Service Order. 
E - Estimate - For all Estimates
N - Normal Read - Scheduled collection of metering data. Also includes the associated Substitutions. 
O - Other - Include ‘Meter Investigation’ & ‘Miscellaneous’ Service Orders. This value is used when providing Historical Data and where the TransCode information is unavailable.
S - Special Read - Special Read’ Service Order.
R - Removal of meter - This is used for meter removal or supply abolishment where the meter has been removed and will not be replaced. This excludes situations involving a meter changeover or where a meter is added to an existing configuration(these are considered to be alterations).

QUALITY FLAGS
A - Actual Metering Data - No method flag is required if this quality flag is used. A reason code is mandatory and must be provided for all Intervals where the meter has recorded a power outage (reason code 79), time reset (reason code 89), or tamper (reason code 61) if this quality flag is used.
E - Forward estimated data - A method flag is mandatory if this quality flag is used. No reason code applies if this quality flag is used.
F - Final substituted data - A method flag is mandatory if this quality flag is used. A reason code is mandatory if this quality flag is used.
S - Substituted data - A method flag is mandatory if this quality flag is used. A reason code is mandatory if this quality flag is used.
V - Variable data - This value is not permitted in NEM13 files. No method flag applies if this quality flag is used. No reason code applies if this quality flag is used. Variable data. This is not a formal quality flag held against individual data items. This value may only be used as part of the QualityMethod field in the 300 record.

