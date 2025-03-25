-- sql/queries/ftp_log_analysis.sql
SELECT
    fl.ip_address,
    COUNT(*) AS file_transfers
FROM
    FTP_Logs fl
GROUP BY
    fl.ip_address
ORDER BY
    file_transfers DESC;