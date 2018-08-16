def init():
    global logging
    logging = []


def generate_logging(flag, message, path):
    """
    Creates logger.logging file
    Args:
        flag: a boolean value indicating if the current run is succeeded or not.
        message: a list of error message
        path: log file location

    Returns:
        NA

    """
    import yaml
    if flag:
        status = "SUCCESS"
    else:
        status = "FAIL"
    file_content = {status: message}
    output_stream = open(path, "w")
    yaml.dump(file_content, output_stream, default_flow_style=False)
    # reset the global logger.logging list
    del logging[:]
    output_stream.close()
