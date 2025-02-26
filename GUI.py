import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
from fte_GUI import process_fte  # Import the main function from fte_GUI.py
from ms_GUI import process_ms    # Import the main function from ms_GUI.py
from modules.kill_switch import terminate_process  # Import the terminate_process event
from modules.logger import data_logger
import threading

"""
Global Parameters
    entry_widget (tk.Entry): The entry widget to update with the selected file path.
    op_plan_path (str): The path to the operational plan file.
    static_report_path (str): The path to the static report file.
    global_staff_path (str): The path to the global staff list file.
    output_directory_path (str): The path to the directory where output files will be saved.
    result (dict): A dictionary to store the result of the process.
"""

def select_file(entry_widget):
    """
    Opens a file dialog to select a file and updates the given entry widget with the selected file path.
    
    Process:
    1. Open a file dialog to select a file.
    2. Clear the current content of the entry widget.
    3. Insert the selected file path into the entry widget.
    4. Log the selected file path using the data_logger.
    """
    file_path = filedialog.askopenfilename()
    entry_widget.delete(0, tk.END)  # Clear current content
    entry_widget.insert(0, file_path)
    data_logger.info(f'Selected file: {file_path}')

def select_directory(entry_widget):
    """
    Opens a directory dialog to select a directory and updates the given entry widget with the selected directory path.
    
    Process:
    1. Open a directory dialog to select a directory.
    2. Clear the current content of the entry widget.
    3. Insert the selected directory path into the entry widget.
    4. Log the selected directory path using the data_logger.
    """
    directory_path = filedialog.askdirectory()
    entry_widget.delete(0, tk.END)  # Clear current content
    entry_widget.insert(0, directory_path)
    data_logger.info(f'Selected directory: {directory_path}')

def run_process_fte(op_plan_path, static_report_path, output_directory_path, result):
    """
    Runs the process for updating the FTE operational plan.
    
    Process:
    1. Log the start of the FTE process.
    2. Call the process_fte function to update the FTE operational plan.
       - If the process completes without termination, set the result to success and log the success message.
    3. Catch any exceptions that occur during the process:
       - Set the result to failure and log the error message.
    4. Ensure that the process finalization is executed in the finally block by calling finalize_process with the result.
    """
    try:
        data_logger.info('Starting process for FTE...')
        process_fte(op_plan_path, static_report_path, output_directory_path, terminate_process)  # Call the function from fte_GUI.py
        data_logger.info('Finished process for FTE.')

        if not terminate_process.is_set():
            result['success'] = True
            result['message'] = f"FTE Op Plan updated successfully! Files saved to {output_directory_path}"
            data_logger.info(result['message'])
    except Exception as e:
        result['success'] = False
        result['message'] = f"An error occurred in the FTE process: {e}"
        data_logger.error(result['message'], exc_info=True)
    finally:
        finalize_process(result)

def run_process_ms(op_plan_path, static_report_path, global_staff_path, output_directory_path, result):
    """
    Runs the process for updating the MS operational plan.
    
    Process:
    1. Log the start of the MS process.
    2. Call the process_ms function to update the MS operational plan.
       - If the process completes without termination, set the result to success and log the success message.
    3. Catch any exceptions that occur during the process:
       - Set the result to failure and log the error message.
    4. Ensure that the process finalization is executed in the finally block by calling finalize_process with the result.
    """
    try:
        data_logger.info('Starting process for MS...')
        process_ms(op_plan_path, static_report_path, global_staff_path, output_directory_path, terminate_process)  # Call the function from ms_GUI.py
        data_logger.info('Finished process for MS.')

        if not terminate_process.is_set():
            result['success'] = True
            result['message'] = f"MS Op Plan updated successfully! Files saved to {output_directory_path}"
            data_logger.info(result['message'])
    except Exception as e:
        result['success'] = False
        result['message'] = f"An error occurred in the MS process: {e}"
        data_logger.error(result['message'], exc_info=True)
    finally:
        finalize_process(result)

def run_process(op_plan_path, static_report_path, global_staff_path, output_directory_path, result):
    """
    Runs the process for updating both the FTE and MS operational plans.
    
    Process:
    1. Log the start of the FTE data process.
    2. Call the process_fte function to update the FTE operational plan.
    3. Log the completion of the FTE data process.
    4. Log the start of the MS data process.
    5. Call the process_ms function to update the MS operational plan.
    6. Log the completion of the MS data process.
    7. If neither process is terminated, set the result to success and log the success message.
    8. Catch any exceptions that occur during the processes:
       - Set the result to failure and log the error message.
    9. Ensure that the process finalization is executed in the finally block by calling finalize_process with the result.
    """
    errors = []
    
    try:
        data_logger.info('Starting data process for FTE...')
        process_fte(op_plan_path, static_report_path, output_directory_path, terminate_process)
        data_logger.info('Finished data process for FTE.')
    except Exception as e:
        errors.append(f"FTE process error: {e}")
        data_logger.error(f"An error occurred in the FTE process: {e}", exc_info=True)
    
    try:
        data_logger.info('Starting data process for MS...')
        process_ms(op_plan_path, static_report_path, global_staff_path, output_directory_path, terminate_process)
        data_logger.info('Finished data process for MS.')
    except Exception as e:
        errors.append(f"MS process error: {e}")
        data_logger.error(f"An error occurred in the MS process: {e}", exc_info=True)
    
    if errors:
        result['success'] = False
        result['message'] = f"Op Plan encountered errors: {'; '.join(errors)}"
    else:
        result['success'] = True
        result['message'] = f"Op Plan updated successfully! Files saved to {output_directory_path}"

    finalize_process(result)

def handle_exception(e):
    """
    Handles exceptions that occur during the execution of the process.
    
    Parameters:
    e (Exception): The exception that was raised.
    
    Process:
    1. Check if the exception message indicates that the process was terminated by the user.
       - If true, log the termination message.
    2. If the exception is not a user termination, display an error message box with the exception details.
       - Log the error message with exception details.
    3. Update the progress label to indicate that an error occurred and suggest checking the log for details.
       - Set the label text to "Error occurred. Check log for details."
       - Set the label text color to red.
    """
    if str(e) == "Process terminated by user.":
        data_logger.info("Process terminated by user.")
    else:
        messagebox.showerror("Error", f"An error occurred: {e}")
        data_logger.error(f"An error occurred: {e}", exc_info=True)
    progress_label.config(text="Error occurred. Check log for details.", foreground="red")

def finalize_process(result):
    """
    Finalizes the process by updating the UI and displaying the result to the user.
    
    Parameters:
    result (dict): A dictionary containing the result of the process, with keys 'success' (bool) and 'message' (str).
    
    Process:
    1. Hide the loading message by removing the progress label from the grid.
    2. Check if the process was successful:
       - If true, display an informational message box with the success message.
       - If false, display an error message box with the error message.
    3. Close the GUI by calling root.quit().
    """
    progress_label.grid_remove()  # Hide the loading message
    if result['success']:
        messagebox.showinfo("Success", result['message'])
    else:
        messagebox.showerror("Error", result['message'])
    root.quit()  # Close the GUI

def on_submit():
    """
    Handles the submission of the file paths and initiates the process for updating the operational plans.
    
    Process:
    1. Clear any existing termination signals.
    2. Retrieve the file paths and output directory path from the input fields.
    3. Check if all required paths (operational plan, static report, and output directory) are provided.
       - If any required paths are missing, display a warning message box and log the warning.
       - Return early to prevent further processing.
    4. Initialize a result dictionary to store the process outcome.
    5. Check if the global staff list path is provided:
       - If not provided, prompt the user with a warning message box to confirm proceeding without the global staff list.
          - If the user chooses to proceed, show the loading message and start a new thread to run the FTE process.
          - If the user chooses not to proceed, return early.
       - If provided, show the loading message and start a new thread to run both the FTE and MS processes.
    """
    terminate_process.clear()

    op_plan_path = op_plan.get()
    static_report_path = static_report.get()
    global_staff_path = global_staff_list.get()
    output_directory_path = output_directory.get()

    if not op_plan_path or not static_report_path or not output_directory_path:
        messagebox.showwarning("Input Error", "Please select all required files and output directory.")
        data_logger.warning("Input Error: Not all files and directories selected.")
        return

    result = {'success': False, 'message': ''}

    if not global_staff_path:
        response = messagebox.askyesno("Warning", "MS process requires Global Staff List, do you still want to proceed?")
        if response:
            progress_label.grid()  # Show the loading message
            update_progress_label(0)
            threading.Thread(target=run_process_fte, args=(op_plan_path, static_report_path, output_directory_path, result)).start()
        else:
            return  # User chose not to proceed
    else:
        progress_label.grid()  # Show the loading message
        update_progress_label(0)
        threading.Thread(target=run_process, args=(op_plan_path, static_report_path, global_staff_path, output_directory_path, result)).start()

def update_progress_label(counter):
    """
    Updates the progress label with a rotating message to indicate ongoing processing.
    
    Parameters:
    counter (int): A counter to determine the current state of the progress message.
    
    Process:
    1. Check if the termination signal is set:
       - If true, return early to stop updating the progress label.
    2. Define a list of text options for the rotating progress message.
    3. Update the progress label text based on the current counter value (modulo 3).
    4. Schedule the next update of the progress label after 1000 milliseconds (1 second) using the root.after method.
       - Increment the counter by 1 for the next update.
    """
    if terminate_process.is_set():
        return
    text_options = ["Processing, please wait.", "Processing, please wait..", "Processing, please wait..."]
    progress_label.config(text=text_options[counter % 3])
    root.after(1000, update_progress_label, (counter + 1))

def on_kill():
    """
    Handles the termination request from the user.
    
    Process:
    1. Set the termination signal to indicate that the process should be terminated.
    2. Log the termination request.
    3. Close the GUI by calling root.quit().
    """
    terminate_process.set()
    data_logger.info("Process termination requested by user.")
    root.quit()  # Close the GUI

# Create the main window
root = tk.Tk()
root.title("Op Plan Automation - Security")
root.configure(bg='#004165')

# Font settings
font_large = ('Arial', 12)
font_button = ('Arial', 12, 'bold')

# Style settings
style = ttk.Style()
style.configure('TLabel', background='#004165', foreground='white', font=font_large)
style.configure('TButton', font=font_button)
style.configure('TEntry', font=font_large)

# Create and place the widgets
ttk.Label(root, text="Op Plan:**").grid(row=0, column=0, padx=10, pady=5)
op_plan = ttk.Entry(root, width=50)
op_plan.grid(row=0, column=1, padx=10, pady=5)
ttk.Button(root, text="Browse", command=lambda: select_file(op_plan), width=10).grid(row=0, column=2, padx=5, pady=5)

ttk.Label(root, text="Static Report:**").grid(row=1, column=0, padx=10, pady=5)
static_report = ttk.Entry(root, width=50)
static_report.grid(row=1, column=1, padx=10, pady=5)
ttk.Button(root, text="Browse", command=lambda: select_file(static_report), width=10).grid(row=1, column=2, padx=5, pady=5)

ttk.Label(root, text="Global Staff List:").grid(row=2, column=0, padx=10, pady=5)
global_staff_list = ttk.Entry(root, width=50)
global_staff_list.grid(row=2, column=1, padx=10, pady=5)
ttk.Button(root, text="Browse", command=lambda: select_file(global_staff_list), width=10).grid(row=2, column=2, padx=5, pady=5)

ttk.Label(root, text="Output Directory:**").grid(row=3, column=0, padx=10, pady=5)
output_directory = ttk.Entry(root, width=50)
output_directory.grid(row=3, column=1, padx=10, pady=5)
ttk.Button(root, text="Browse", command=lambda: select_directory(output_directory), width=10).grid(row=3, column=2, padx=5, pady=5)

ttk.Button(root, text="Submit", command=on_submit, width=10).grid(row=4, column=1, pady=20)
ttk.Button(root, text="Stop", command=on_kill, width=10).grid(row=4, column=2, pady=20)

# Add a label to show progress, initially hidden
progress_label = ttk.Label(root, text="", foreground="red")
progress_label.grid(row=5, column=1, pady=10)
progress_label.grid_remove()  # Hide the loading message initially

# Start the GUI event loop
root.mainloop()