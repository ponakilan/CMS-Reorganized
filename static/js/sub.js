document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("user-form");
    const tableSection = document.getElementById("table-section");
    const tableHeading = document.getElementById("table-heading");
    const dataTable = document.getElementById("data-table");
    const tableBody = dataTable.querySelector("tbody");

    form.addEventListener("submit", async (event) => {
        event.preventDefault();

        // Get the username from the input field
        const username = document.getElementById("username").value.trim();
        if (!username) {
            alert("Please enter a username.");
            return;
        }

        try {
            // Make a GET request with the username as a query parameter
            const response = await fetch(`/submitted-jobs?username=${encodeURIComponent(username)}`);
            if (!response.ok) {
                throw new Error("Failed to fetch data from the server.");
            }

            // Parse the JSON data
            const data = await response.json();

            // Clear the previous table data
            tableBody.innerHTML = "";

            if (data.jobs && data.jobs.length > 0) {
                // Populate the table with the jobs data
                data.jobs.forEach(job => {
                    const row = document.createElement("tr");
                    row.innerHTML = `
                        <td>${job.username}</td>
                        <td>${job.job_id}</td>
                        <td>${job.in_time}</td>
                        <td>${job.out_time}</td>
                    `;
                    tableBody.appendChild(row);
                });

                // Show the table and heading
                tableHeading.style.display = "block";
                dataTable.style.display = "table";
            } else {
                // If no jobs found, hide the table and display a message
                tableHeading.style.display = "block";
                dataTable.style.display = "none";
                tableHeading.textContent = `No jobs found for username: ${username}`;
            }
        } catch (error) {
            console.error(error);
            alert("An error occurred while fetching jobs data.");
        }
    });
});
