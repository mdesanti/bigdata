$ ->
  $(document).ready ->
    Highcharts.setOptions global:
      useUTC: false

    window.parties = ['pro', 'unen', 'otros', 'frente renovador', 'fpv']

    chart = undefined
    $("#container").highcharts
      chart:
        type: "spline"
        animation: Highcharts.svg # don't animate in old IE
        marginRight: 10
        events:
          load: ->
            
            # set up the updating of the chart each second
            series = @series
            setInterval (->
              i = 0
              console.log window.parties.lenth
              while i <= (window.parties.lenth - 1)
                $.ajax(
                  url: "tweets?name=" + window.parties[i]
                ).done (data) ->
                  console.log data
                  x = (new Date()).getTime() # current time
                  y = Math.random()
                  series[i].addPoint [x+i, y+i], true, false
                i++
            ), 1000

      title:
        text: "Live random data"

      xAxis:
        type: "datetime"
        tickPixelInterval: 150

      yAxis:
        title:
          text: "Value"

        plotLines: [
          value: 0
          width: 1
          color: "#808080"
        ]

      tooltip:
        formatter: ->
          "<b>" + @series.name + "</b><br/>" + Highcharts.dateFormat("%Y-%m-%d %H:%M:%S", @x) + "<br/>" + Highcharts.numberFormat(@y, 2)

      legend:
        enabled: false

      exporting:
        enabled: false

      series: [
        { name: "Random data"
        data: (->
          
          # generate an array of random data
          data = []
          time = (new Date()).getTime()
          i = 1
          data.push
            x: time + i * 1000
            y: Math.random()

          data
        )() },
        { name: "Random data 2"
        data: (->
          
          # generate an array of random data
          data = []
          time = (new Date()).getTime()
          i = 1
          data.push
            x: time + i * 1000
            y: Math.random()

          data
        )()}
      ]


