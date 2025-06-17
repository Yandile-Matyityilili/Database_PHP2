<?php
// index.php
// This is your main RFID Attendance Dashboard frontend.

include('config.php'); // Include your database connection file
session_start(); // Start the session for cooldown tracking

$cooldown_seconds = 0.5; // Cooldown period in seconds (0.5 seconds = 300 milliseconds as per Python backend)

// --- Function to parse Python script output and determine message type ---
function parsePythonOutput($output) {
    $message = '';
    $type = 'info'; // Default type

    // Define regex patterns for different log levels from Python's logging module
    // INFO, WARNING, ERROR, CRITICAL
    if (preg_match('/^.*? - INFO - (.*)$/m', $output, $matches)) {
        $message = trim($matches[1]);
        $type = 'success'; // Treat INFO as success for user feedback
    }
    if (preg_match('/^.*? - WARNING - (.*)$/m', $output, $matches)) {
        $message .= ($message ? "<br>" : "") . trim($matches[1]);
        $type = 'warning';
    }
    if (preg_match('/^.*? - ERROR - (.*)$/m', $output, $matches)) {
        $message .= ($message ? "<br>" : "") . trim($matches[1]);
        $type = 'error';
    }
    if (preg_match('/^.*? - CRITICAL - (.*)$/m', $output, $matches)) {
        $message .= ($message ? "<br>" : "") . trim($matches[1]);
        $type = 'error'; // Critical errors are also errors
    }

    if (empty($message)) {
        // If no specific log message caught, show generic success or default output
        if (strpos($output, 'processed successfully') !== false) {
             $message = "RFID tag processed successfully.";
             $type = 'success';
        } elseif (strpos($output, 'Failed to process tag') !== false) {
            $message = "Failed to process RFID tag. Please check backend logs.";
            $type = 'error';
        } else {
             $message = "Python script executed. Check console for details.";
             $type = 'info';
        }
    }

    return ['message' => $message, 'type' => $type, 'raw_output' => $output];
}

// --- Process RFID Tag Submission ---
$display_message = null;
$python_raw_output = '';

if ($_SERVER["REQUEST_METHOD"] == "POST" && isset($_POST["tag_id"])) {
    // Sanitize input - allow only alphanumeric characters
    $tag_id = preg_replace('/[^a-zA-Z0-9]/', '', $_POST["tag_id"]);

    $now = microtime(true); // Use microtime for higher precision
    $last_scan = $_SESSION['last_scan'][$tag_id] ?? 0;

    // Check cooldown period
    if (($now - $last_scan) < $cooldown_seconds) {
        $display_message = [
            'message' => "Please wait " . round($cooldown_seconds - ($now - $last_scan), 1) . " seconds before scanning this tag again.",
            'type' => 'warning'
        ];
    } else {
        // Update last scan time in session
        $_SESSION['last_scan'][$tag_id] = $now;

        // --- Execute Python Script ---
        // IMPORTANT: Verify these paths are absolutely correct for your XAMPP/Windows setup
        $python_executable = "C:/Users/Intern-28/AppData/Local/Programs/Python/Python313/python.exe"; // Your Python executable path
        $python_script = "C:/xampp/htdocs/attendance_system/python/attendance.py"; // Your Python script path

        // Build command with escaped tag_id argument
        // 2>&1 redirects stderr to stdout so PHP captures all output
        $cmd = escapeshellarg($python_executable) . " " . escapeshellarg($python_script) . " " . escapeshellarg($tag_id) . " 2>&1";

        // Execute the Python script and capture output
        $python_raw_output = shell_exec($cmd);

        // Parse the Python output for user-friendly messages
        $display_message = parsePythonOutput($python_raw_output);

        // Optional: Redirect to clear POST data and prevent resubmission on refresh
        // This will also ensure the attendance table updates.
        // It's common to store the message in a session variable before redirecting.
        $_SESSION['flash_message'] = $display_message;
        header("Location: " . $_SERVER['PHP_SELF']);
        exit();
    }
}

// Check for a flash message from a previous redirect
if (isset($_SESSION['flash_message'])) {
    $display_message = $_SESSION['flash_message'];
    unset($_SESSION['flash_message']); // Clear the flash message after displaying
}

?>
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RFID Attendance Dashboard</title>
    <link rel="stylesheet" href="style.css"> 
</head>
<body>
    <h1>Attendance Today (<?php echo date('Y-m-d'); ?>)</h1>

    <form method="POST" action="">
        <label for="tag_id">Enter Tag ID:</label>
        <input type="text" name="tag_id" id="tag_id" required autofocus>
        <button type="submit">Scan Tag</button>
    </form>

    <?php if ($display_message): ?>
        <div class="message <?php echo htmlspecialchars($display_message['type']); ?>">
            <?php echo nl2br(htmlspecialchars($display_message['message'])); ?>
        </div>
        <?php if (!empty($display_message['raw_output'])): ?>
            <p class="message info">Raw Python Script Output (for debugging):</p>
            <pre><?php echo htmlspecialchars($display_message['raw_output']); ?></pre>
        <?php endif; ?>
    <?php endif; ?>

    <h2>Current Attendance</h2>
    <table>
        <thead>
            <tr>
                <th>Name</th>
                <th>Department</th>
                <th>Status</th>
                <th>Last Scan Time</th>
            </tr>
        </thead>
        <tbody>
            <?php
            // --- Fetch and Display Live Attendance from MySQL ---
            $today_date = date('Y-m-d');
            $sql = "SELECT s.Name, s.Dept, d.Status, d.Time 
                    FROM Staff s
                    LEFT JOIN DailyAttendance d ON s.Name = d.Name AND d.Date = '$today_date'
                    ORDER BY s.Name ASC";


            // Get all staff
            $staff_query = "SELECT ID, Name, Dept FROM Staff ORDER BY Name ASC";
            $staff_result = $conn->query($staff_query);

            if ($staff_result && $staff_result->num_rows > 0) {
                while ($staff_row = $staff_result->fetch_assoc()) {
                    $staff_name = $staff_row['Name'];
                    $staff_dept = $staff_row['Dept'];
                    $status = 'OUT'; // Default status
                    $scan_time = 'N/A';

                    // Check latest entry for this staff member today in 'onsite'
                    $onsite_query = "SELECT scan_date, is_arriving FROM onsite 
                                     WHERE assigned_to = ? AND DATE(scan_date) = CURDATE()
                                     ORDER BY scan_date DESC LIMIT 1";
                    $stmt = $conn->prepare($onsite_query);
                    $stmt->bind_param("s", $staff_name);
                    $stmt->execute();
                    $onsite_result = $stmt->get_result();

                    if ($onsite_result && $onsite_result->num_rows > 0) {
                        $onsite_entry = $onsite_result->fetch_assoc();
                        if ($onsite_entry['is_arriving'] == 1) {
                            $status = 'IN';
                        } else {
                            $status = 'OUT';
                        }
                        $scan_time = (new DateTime($onsite_entry['scan_date']))->format('H:i:s');
                    } else {
                        // If no entry today, they are considered Absent or OUT.
                        // The Python script marks Absent in Monthly Sheet if not scanned.
                        // For daily dashboard, no entry means effectively 'OUT' from a tap perspective.
                        $status = 'Absent';
                    }
                    $stmt->close();
            ?>
                    <tr>
                        <td><?php echo htmlspecialchars($staff_name); ?></td>
                        <td><?php echo htmlspecialchars($staff_dept); ?></td>
                        <td class="status-<?php echo strtolower($status); ?>"><?php echo htmlspecialchars($status); ?></td>
                        <td><?php echo htmlspecialchars($scan_time); ?></td>
                    </tr>
            <?php
                }
            } else {
                echo '<tr><td colspan="4">No staff data found.</td></tr>';
            }
            // Close the database connection (handled in config.php's scope, but good to be explicit if needed elsewhere)
            // $conn->close(); // Only if this is the last use of $conn
            ?>
        </tbody>
    </table>
<style>
    /* style.css */
body {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    margin: 20px;
    background-color: #e9f5ff; /* Light blue background */
    color: #333;
    line-height: 1.6;
}

h1 {
    color: #0056b3;
    text-align: center;
    margin-bottom: 30px;
    font-size: 2.5em;
    text-shadow: 1px 1px 2px rgba(0,0,0,0.1);
}

form {
    background-color: #ffffff;
    padding: 25px;
    border-radius: 12px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    margin: 0 auto 30px auto;
    max-width: 500px;
    display: flex;
    flex-direction: column;
    gap: 15px;
    border: 1px solid #e0e0e0;
}

label {
    font-weight: bold;
    color: #555;
    font-size: 1.1em;
}

input[type="text"] {
    padding: 12px;
    border: 1px solid #a0c4e2; /* Softer blue border */
    border-radius: 8px;
    font-size: 1.1em;
    box-shadow: inset 0 1px 3px rgba(0,0,0,0.05);
    transition: border-color 0.3s ease;
}

input[type="text"]:focus {
    border-color: #007bff;
    outline: none;
    box-shadow: 0 0 0 3px rgba(0,123,255,0.25);
}

button {
    background-color: #007bff;
    color: white;
    padding: 12px 20px;
    border: none;
    border-radius: 8px;
    cursor: pointer;
    font-size: 1.2em;
    font-weight: bold;
    transition: background-color 0.3s ease, transform 0.1s ease;
    box-shadow: 0 2px 5px rgba(0,0,0,0.2);
}

button:hover {
    background-color: #0056b3;
    transform: translateY(-1px);
}

button:active {
    transform: translateY(0);
    box-shadow: 0 1px 3px rgba(0,0,0,0.2);
}

.message {
    padding: 12px;
    margin: 15px auto;
    border-radius: 8px;
    font-weight: bold;
    max-width: 500px;
    text-align: center;
    border: 1px solid transparent;
}

.message.success {
    background-color: #d4edda;
    color: #155724;
    border-color: #c3e6cb;
}

.message.warning {
    background-color: #fff3cd;
    color: #856404;
    border-color: #ffeeba;
}

.message.error {
    background-color: #f8d7da;
    color: #721c24;
    border-color: #f5c6cb;
}

.message.info {
    background-color: #d1ecf1;
    color: #0c5460;
    border-color: #bee5eb;
}

pre {
    background-color: #f8f9fa; /* Lighter background for code/logs */
    padding: 15px;
    border-radius: 8px;
    white-space: pre-wrap;
    word-wrap: break-word;
    margin: 20px auto;
    font-size: 0.9em;
    max-width: 800px;
    border: 1px solid #e0e0e0;
    box-shadow: inset 0 1px 3px rgba(0,0,0,0.05);
}

table {
    width: 90%;
    border-collapse: collapse;
    margin: 30px auto;
    background-color: #ffffff;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    border-radius: 12px;
    overflow: hidden; /* Ensures rounded corners apply to content */
    border: 1px solid #e0e0e0;
}

th, td {
    padding: 14px 20px;
    text-align: left;
    border-bottom: 1px solid #e9ecef; /* Lighter border for rows */
}

th {
    background-color: #007bff;
    color: white;
    text-transform: uppercase;
    font-size: 0.95em;
    letter-spacing: 0.05em;
    position: sticky;
    top: 0;
    z-index: 1;
}

tr:nth-child(even) {
    background-color: #f8f9fa; /* Alternate row shading */
}

tr:hover {
    background-color: #e2f0ff; /* Lighter blue on hover */
    transition: background-color 0.2s ease;
}

.status-in {
    color: #28a745; /* Green */
    font-weight: bold;
}

.status-out {
    color: #dc3545; /* Red */
    font-weight: bold;
}

.status-absent {
    color: #6c757d; /* Gray */
    font-style: italic;
}

/* Responsive adjustments */
@media (max-width: 768px) {
    h1 {
        font-size: 2em;
    }
    form {
        margin: 20px auto;
        max-width: 90%;
    }
    input[type="text"] {
        font-size: 1em;
        padding: 10px;
    }
    button {
        font-size: 1.1em;
        padding: 10px 15px;
    }
    table {
        width: 95%;
        font-size: 0.9em;
    }
    th, td {
        padding: 10px 12px;
    }
}
</style>
</body>
</html>