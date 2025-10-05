def pytest_addoption(parser):
    parser.addoption("--chrome", action="store", default="chrome")
    parser.addoption("--chromedriver", action="store", default="chromedriver")