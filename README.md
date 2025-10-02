# REST API Analysis service module

Designed to be run using docker with application specific code mixed in by shoestring assembler.

The solution must provide the following to the module:
- In `solution_config/source_config/analysis/`:
  - `analysis_modules/run.py`  
  Effectively the `main` for the module.

  - `module_config/module_config.toml`  
    In PowerMonitoring this looked like:
<ul><ul>
  
```
[influx]
	url = "http://timeseries-db.docker.local:8086"
	token = "<replaced_by_env_variable>"
	org = "SHOESTRING"	
	bucket = "power_monitoring"
```
</ul></ul>
