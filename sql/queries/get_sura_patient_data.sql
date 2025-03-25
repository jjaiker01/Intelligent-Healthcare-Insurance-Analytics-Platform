-- sql/queries/get_company_patient_data.sql
CREATE PROCEDURE GetcompanyPatientData
AS
BEGIN
    SELECT 
        PatientID, 
        PatientName, 
        Age, 
        Address, 
        MedicalHistory
    FROM 
        companyDatabase.dbo.Patients; -- Replace with your actual table name
END;