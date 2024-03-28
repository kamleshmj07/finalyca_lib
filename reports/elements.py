from jinja2 import Environment, FileSystemLoader

def get_table_from_html_template(data_dict, header_sequence_list, width):    
    environment = Environment(loader=FileSystemLoader('./templates'), keep_trailing_newline=False,trim_blocks=True, lstrip_blocks=True)
    
    data = dict()        
    data["data_dict"] = data_dict
    data["header_list"] = header_sequence_list
    data["table_width"] = round((width * 720 / 100),0)  

    template = environment.get_template('table_component.html')
    html_out = template.render(data)
    
    return html_out