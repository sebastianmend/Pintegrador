import doobie.*
import doobie.implicits.*
import cats.*
import cats.effect.*
import cats.implicits.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import breeze.plot.{Figure, plot}
import com.github.tototoshi.csv.*
import doobie.implicits.toSqlInterpolator
import java.io.File
import org.nspl.*
import org.nspl.data.HistogramData
import org.saddle.{Index, Series, Vec}
import org.nspl.awtrenderer.*
import org.nspl._
import org.nspl.saddle._
import org.nspl.awtrenderer._
import org.nspl.data.*
import org.nspl._
import org.nspl.awtrenderer._
import org.nspl.data._

object ProyectoBimestral {
  @main
  def main() ={
    val path2DataFile: String = "C:\\Users\\jeana\\Desktop\\Practicum\\dsPartidosYGoles.csv"
    val path2DataFile2: String = "C:\\Users\\jeana\\Desktop\\Practicum\\dsAlineacionesXTorneo.csv"
    val reader1: CSVReader = CSVReader.open(new File(path2DataFile))
    val reader2: CSVReader = CSVReader.open(new File(path2DataFile2))

    val contentFile: List[Map[String, String]] = reader1.allWithHeaders()
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()
    reader1.close()
    reader2.close()

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/proyecto_integrador",
      user = "root",
      password = "RocketManProtocol621@",
      logHandler = None
    )

    def defaultValue(text: String): Double =
      if (text.equals("NA")) {
        0
      } else {
        text.toDouble
      }

    val listaCapacidad: List[Int] = contentFile
      .distinctBy(_("matches_stadium_id"))
      .map(_("stadiums_stadium_capacity").toInt)

    val listaNombre: List[String] = contentFile
      .distinctBy(_("matches_stadium_id"))
      .map(_("stadiums_stadium_name"))

    val listaEstadiosYCapacidad: List[(String, Int)] = listaNombre.zip(listaCapacidad)

    def minimo(listaT: List[(String, Int)]): (String, Int) = {
      listaT.minBy(_._2)
    }

    def maximo(listaT: List[(String, Int)]): (String, Int) = {
      listaT.maxBy(_._2)
    }

    def promedio(listaCapacidades: List[Int]): Double = {
      listaCapacidades.sum.toDouble / listaCapacidades.length
    }

    def frecuencia(listaCapacidades: List[Int]): Int = {
      listaCapacidades.reduceLeft(Math.max)
    }

    def rango(listaCapacidades: List[Int]): Int = {
      listaCapacidades.max - listaCapacidades.min
    }

    val min = minimo(listaEstadiosYCapacidad)
    println(s"${min._1} es el estadio con la menor capacidad, contando con una capacidad de ${min._2} ")
    val max = maximo(listaEstadiosYCapacidad)
    println(s"${max._1} es el estadio con la mayor capacidad, contando con una capacidad de ${max._2} ")
    println("La capacidad promedio de los estadios es: " + promedio(listaCapacidad))
    println("La capacidad mas frecuente entre estadios es: " + frecuencia(listaCapacidad))
    println("Rango de variabilidad entre las capacidades de los estadios: " + rango(listaCapacidad))

    //Graficos por lectura de csv

    graficoCamisetas(contentFile2)
    def graficoCamisetas(data: List[Map[String, String]]): Unit = {
      // Filtrar y mapear los datos
      val camisetas = data.map(_("squads_shirt_number"))
        .filterNot(_.equals("NA"))
        .map(_.toDouble)

      // Crear el histograma
      val histogramaCamisetas = xyplot(HistogramData(camisetas, 25) -> line())(
        par
          .xlab("Numero de camiseta")
          .ylab("Frecuencia")
          .main("Numero de camiseta mas frecuente")
      )

      // Guardar el gráfico
      pngToFile(new File("C:\\Users\\jeana\\Desktop\\Graficas\\graficaFNCamista.png"), histogramaCamisetas.build, width = 1000)
    }

    graficoGenero(contentFile2)
    def graficoGenero(data: List[Map[String, String]]): Unit = {
      // Filtrar y mapear los datos
      val generos = data.map(_("players_female"))
        .filterNot(_.equals("NA"))
        .map(_.toDouble)

      // Crear el histograma
      val histogramaGenero = xyplot(HistogramData(generos, 2) -> bar())(
        par
          .xlab("Genero (0 Masculino, 1 Femenino)")
          .ylab("Cantidad")
          .main("Cantidad de jugadores hombres y mujeres")
      )

      // Guardar el gráfico
      pngToFile(new File("C:\\Users\\jeana\\Desktop\\Graficas\\graficaFGenero.png"), histogramaGenero.build, width = 1000)
    }

    graficoPartidosXAnio(contentFile)
    def graficoPartidosXAnio(data: List[Map[String, String]]): Unit = {
      // Filtrar y mapear los datos
      val anios = data.map(_("tournaments_year"))
        .filterNot(_.equals("NA"))
        .map(_.toDouble)

      // Crear el histograma
      val PartidosAnio = xyplot(HistogramData(anios, 25) -> line())(
        par
          .xlab("Anios")
          .ylab("Numero de Partidos")
          .main("Cantidad de partidos jugados por anio")
      )

      // Guardar el gráfico
      pngToFile(new File("C:\\Users\\jeana\\Desktop\\Graficas\\graficaFPartidos.png"), PartidosAnio.build, width = 1000)
    }

    //Graficos por lectura de la base de datos

    graficaPosicionesRepeticiones(pocicionRepeticiones.transact(xa).unsafeRunSync())
    def pocicionRepeticiones: ConnectionIO[List[(String, Double)]] =
      sql"""
           |SELECT squads_position_name, CAST(COUNT(*) AS REAL) AS repeticiones
           |FROM squads
           |GROUP BY squads_position_name
           |""".stripMargin
        .query[(String, Double)]
        .to[List]

    def graficaPosicionesRepeticiones(data: List[(String, Double)]): Unit = {
      val indices = Index(data.map(_._1).toArray)
      val values = Vec(data.map(_._2).toArray)

      val series = Series(indices, values)

      val barPlot = saddle.barplotHorizontal(series,
        xLabFontSize = Option(RelFontSize(0.5))
      )(
        par
          .xLabelRotation(-120)
          .xNumTicks(0)
          .xlab("Posiciones")
          .ylab("Frecuencia")
          .main("Posiciones con mas jugadores a lo largo del tiempo")
      )
      pngToFile(
        new File("C:\\Users\\jeana\\Desktop\\Graficas\\graficaPosicionF.png"), barPlot.build, 5000)
    }

    graficaGolesXMinuto(golesXMinuto.transact(xa).unsafeRunSync())
    def golesXMinuto: ConnectionIO[List[(Int, Int)]] =
      sql"""
           |SELECT goals_minute_regulation, COUNT(DISTINCT goals_goal_id) AS goles
           |FROM goals
           |group by goals_minute_regulation;
           |""".stripMargin
        .query[(Int, Int)]
        .to[List]

    def graficaGolesXMinuto(data: List[(Int, Int)]): Unit = {
      val f = Figure()
      val p = f.subplot(0)
      p += plot(data.map(_._1), data.map(_._2), '+')
      p.xlabel = "Minutos"
      p.ylabel = "Goles"
      f.saveas("C:\\Users\\jeana\\Desktop\\Graficas\\graficaGolesXMinuto.png")
    }

    graficaGollesXMinuto(golesTiempoExtravsMinulto.transact(xa).unsafeRunSync())
    def golesTiempoExtravsMinulto: ConnectionIO[List[(String, Int, Int)]] =
      sql"""
           |SELECT matches_match_id,
           |       SUM(matches_home_team_score + matches_away_team_score) AS goles_tiempo_extra,
           |       SUM(matches_home_team_score_penalties + matches_away_team_score_penalties) AS goles_en_penales
           |FROM matches
           |GROUP BY matches_match_id;
           |""".stripMargin
        .query[(String, Int, Int)]
        .to[List]

    def graficaGollesXMinuto(data: List[(String, Int, Int)]): Unit = {
      val f = Figure()
      val p = f.subplot(0)
      p += plot(data.map(_._2), data.map(_._3), '+')
      p.xlabel = "goles tiempo extra"
      p.ylabel = "Goles en penales"
      f.saveas("C:\\Users\\jeana\\Desktop\\Graficas\\graficagolesTEVSPealty.png")
    }

  }
}
