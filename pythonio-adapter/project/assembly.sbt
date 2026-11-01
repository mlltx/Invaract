// The checker (PythonIoCheck) is a program of its own, run by the Python package: it is assembled into one jar, which the
// native-image build (CI's pythonio-native-checker job) compiles ahead of time into a native program.
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
