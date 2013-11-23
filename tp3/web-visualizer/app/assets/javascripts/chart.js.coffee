$ ->
  $(document).ready ->
    Highcharts.setOptions global:
      useUTC: false

    window.parties = ['pro', 'unen', 'otros', 'frente renovador', 'fpv']

    chart = undefined
    $("#container").highcharts

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
        { name: 'pro', data: []},
        { name: 'unen', data: []},
        { name: 'otros', data: []},
        { name: 'frente renovador', data: []},
        { name: 'fpv', data: []}
      ]

      chart:
        type: "spline"
        animation: Highcharts.svg # don't animate in old IE
        marginRight: 10
        events:
          load: ->
            
            # set up the updating of the chart each second
            index = 0
            while index <= (window.parties.length - 1)
              serie = this.series[index]
              party = window.parties[index]
              setInterval (->
                console.log('Before request for ' + party)
                $.ajax(
                  url: "tweets?name=" + party
                ).done (data) ->
                  x = (new Date()).getTime() # current time
                  y = data.charts[0].quantity
                  serie.addPoint [x, y], true, false
              ), 3000
              index++


