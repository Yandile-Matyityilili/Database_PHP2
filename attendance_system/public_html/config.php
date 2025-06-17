<?php
// config.php
// This file establishes the MySQL database connection for your PHP frontend.

// Database credentials - ENSURE THESE MATCH your .env file in the Python directory
// (MYSQL_HOST, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD)
$db_host = 'localhost'; // Should match MYSQL_HOST in .env
$db_user = 'root';    // Should match MYSQL_USER in .env
$db_password = 'Ursw!nF@r0'; // Should match MYSQL_PASSWORD in .env
$db_name = 'attendance_db'; // Should match MYSQL_DATABASE in .env

// Create database connection
$conn = new mysqli($db_host, $db_user, $db_password, $db_name);

// Check connection
if ($conn->connect_error) {
    // Log the connection error (instead of dying silently in production)
    error_log("Database Connection Failed: " . $conn->connect_error);
    // Display a user-friendly message
    die("<p style='color: red; font-weight: bold;'>Unable to connect to the database. Please try again later.</p>");
}
?>