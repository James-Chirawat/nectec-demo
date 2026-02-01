SELECT a.title, SUM(t.unit_price) as album_value
FROM album a
JOIN track t ON a.album_id = t.album_id
GROUP BY a.album_id
ORDER BY album_value DESC
LIMIT 5;

SELECT billing_country, SUM(total) as total_revenue, COUNT(invoice_id) as sales_count
FROM invoice
GROUP BY billing_country
ORDER BY total_revenue DESC;

SELECT 
    e.first_name || ' ' || e.last_name AS employee,
    m.first_name || ' ' || m.last_name AS reports_to_manager
FROM employee e
LEFT JOIN employee m ON e.reports_to = m.employee_id;