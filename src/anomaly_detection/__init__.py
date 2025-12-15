# Initialize data validator if enabled
self.data_validator = None
validation_config = self.config.get('validation', {})
if validation_config.get('enabled', False):
    self.data_validator = get_data_validator(self.config)
    logger.info("Data validation enabled")