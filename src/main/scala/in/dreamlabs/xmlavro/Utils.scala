package in.dreamlabs.xmlavro

import scala.reflect.io.Path

/**
  * Created by Royce on 21/12/2016.
  */
object Utils {
  def replaceExtension(sourcePath: Path, newExtension: String): Path = sourcePath changeExtension newExtension

  def replaceExtension(sourcePath: String, newExtension: String): Path = Path apply sourcePath changeExtension newExtension

  def replaceBaseDir(sourcePath: String, baseDir: Path): Path = {
    replaceBaseDir(Path apply sourcePath, baseDir)
  }

  def replaceBaseDir(sourcePath: Path, baseDir: Path): Path = {
    if (baseDir == null)
      sourcePath
    else if (sourcePath isAbsolute)
      sourcePath
    else
      sourcePath toAbsoluteWithRoot baseDir
  }

}