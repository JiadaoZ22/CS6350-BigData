object readfile {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val plot_path = path+"/plot_summaries.txt"
    val ml_path = path+"/movie_metadata.tsv"

    println("plot_path: ", plot_path)
    println("ml_path: ", ml_path)
    println("args: ", args.slice(1,args.length))

    return
  }
}
