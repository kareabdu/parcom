package com.sonatel.parc

import java.io.File

package object datastream  {

  def apply() : List[File] = {
    SelectFilesParcs.getFiles("")
  }
}
