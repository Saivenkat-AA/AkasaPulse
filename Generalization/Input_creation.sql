SELECT 
	  CONCAT(SS.Month,SS.Year,SS.Segment,SS.Functions,SS.Department) AS Unique_ID,
    SS.Feedback,
	  CS.Category_Name,
	  CS.Sentiments,
    SS.Month,
    SS.Year,
    SS.Segment,
    SS.Functions,
    SS.Department
FROM QP_DA.Category_Sentiment CS
LEFT JOIN 
	(
		SELECT 
			Respondent_ID, 
			Verbatim AS Feedback, 
			Month, 
			Year, 
            CASE
                WHEN Department LIKE 'CEO%' THEN 'CEO Office & Others'
                WHEN Department LIKE '%AALA%' THEN 'AALA'
                WHEN Department LIKE 'coo%' THEN 'COO Office'
                WHEN Department LIKE 'Marketing%' THEN 'Marketing | Corp Comms | eCommerce | Customer Exp'
                ELSE Department
            END Functions, 
            CASE
                WHEN SubDepartment = 'AALA (Akasa Air Learning Academy)' THEN 'AALA'
                WHEN SubDepartment = 'aala administration' THEN 'AALA Administration'
                WHEN SubDepartment = 'aala data analytics' THEN 'AALA Data Analytics'
                WHEN SubDepartment = 'aeropolitical & industry affairs' THEN 'Areopolitical & Industry Affairs'
                WHEN SubDepartment = 'aps training' THEN 'APS Training'
                WHEN SubDepartment LIKE 'CEO%' THEN 'CEO Office'
                WHEN SubDepartment LIKE 'Compensation%' THEN 'Compensation, Benefits & Compliance'
                WHEN SubDepartment = 'communication training' THEN 'Communication Training'
                WHEN SubDepartment LIKE 'coo%' THEN 'COO Office'
                WHEN SubDepartment = 'customer loyalty and partnerships' THEN 'Customer Loyalty and Partnerships'
                WHEN SubDepartment = 'dgr training' THEN 'DGR Training'
                WHEN SubDepartment = 'operations excellence' THEN 'Operations Excellence'
                WHEN SubDepartment LIKE 'grooming%' THEN 'Grooming Training'
                WHEN SubDepartment LIKE 'ifs%' THEN 'IFS Training'
                ELSE SubDepartment
            END AS Department,
			Segment,
			EmailFilter
		FROM QP_DA.eNPSSurveyData
	)SS
ON CS.Respondent_ID = SS.Respondent_ID
WHERE Month = 12 AND Year = 2025 -- Change the Month and Year according to the requirement
;

/*
After extracting the output, Tranform the Category_Name to Category_ID as below:
    "Compensation": 1,
    "Employee Policies & Benefits": 2,
    "Career & Growth": 3,
    "Reward and Recognition": 4,
    "Learning & Development": 5,
    "Work Place Amenities": 6,
    "Work Environment & Culture": 7,
    "Operational Effectiveness": 8,
    "Leadership": 9,
    "Line Manager": 10,
    "Team": 11,
    "Cross functional Collaboration": 12,
    "Pride and Brand association": 13,
    "Others": 14
*/


