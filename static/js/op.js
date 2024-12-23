document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("categories-form");
    const container = document.getElementById("categories-container");
    const heading = document.querySelector("h1");

    let selectedOname = [];
    let selectedRename = [];

    async function fetchOpenPayData() {
        try {
            const response = await fetch("/openpay-cat");
            if (!response.ok) throw new Error("Failed to fetch data");
            const data = await response.json();

            populateCategories(data.categories);
        } catch (error) {
            console.error("Error fetching OpenPay data:", error);
        }
    }

    function populateCategories(categories) {
        categories.forEach(category => {
            const formGroup = document.createElement("div");
            formGroup.classList.add("mb-3");

            const label = document.createElement("label");
            label.textContent = category.Original;
            label.classList.add("form-label");
            formGroup.appendChild(label);

            const input = document.createElement("input");
            input.type = "text";
            input.name = "renamed";
            input.classList.add("form-control");
            input.value = category.Renamed || "";
            input.dataset.original = category.Original;
            formGroup.appendChild(input);

            container.appendChild(formGroup);
        });
    }

    function setupDrugInput() {
        // Clear existing inputs and change the heading
        container.innerHTML = "";
        heading.textContent = "Enter Drugs";

        const formGroup = document.createElement("div");
        formGroup.classList.add("mb-3");

        const label = document.createElement("label");
        label.textContent = "Enter Drugs (Comma seperated):";
        label.classList.add("form-label");
        formGroup.appendChild(label);

        const input = document.createElement("input");
        input.type = "text";
        input.name = "drugs";
        input.classList.add("form-control");
        formGroup.appendChild(input);

        container.appendChild(formGroup);
    }

    form.addEventListener("submit", async (event) => {
        event.preventDefault();

        const inputs = container.querySelectorAll("input[name='renamed']");

        // If this is the initial form submission
        if (inputs.length > 0) {
            inputs.forEach(input => {
                selectedOname.push(input.dataset.original);
                selectedRename.push(input.value);
            });

            // Set up for drug input
            setupDrugInput();
        } else {
            // Handle drug input submission
            const drugInput = container.querySelector("input[name='drugs']");
            const drugs = drugInput.value.split(",").map(drug => drug.trim());

            // Send all data to the backend
            await fetch("/openpay-data", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    selected_oname: selectedOname,
                    selected_rename: selectedRename,
                    drugs: drugs,
                }),
            });
        }
    });

    fetchOpenPayData();
});
