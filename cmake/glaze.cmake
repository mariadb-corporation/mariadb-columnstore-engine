include(FetchContent)

FetchContent_Declare(
  glaze
  GIT_REPOSITORY https://github.com/stephenberry/glaze.git
  GIT_TAG v5.7.0
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(glaze)