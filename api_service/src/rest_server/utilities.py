class RestUrlPathUtilities:

    """Implements utilities for REST service API URL's"""

    _PATH_SEPARATOR = "/"
    _URL_PARAMETER_TEMPLATE = "<%s>"

    @staticmethod
    def url_path(*url_path_components):
        """Returns URL path generated from given URL components"""
        return RestUrlPathUtilities._PATH_SEPARATOR.join(url_path_components)

    @staticmethod
    def url_path_with_leading_path_separator(*url_path_components):
        """Returns URL path generated from given URL components with a leading separator"""
        return RestUrlPathUtilities._PATH_SEPARATOR + RestUrlPathUtilities.url_path(*url_path_components)

    @staticmethod
    def url_path_with_parameters(url_path, *path_parameters):
        """Returns URL path with URL parameters with a leading separator"""
        return RestUrlPathUtilities.url_path_with_leading_path_separator(
            url_path, *map(
                lambda pathParameter: RestUrlPathUtilities._URL_PARAMETER_TEMPLATE % pathParameter, path_parameters))
