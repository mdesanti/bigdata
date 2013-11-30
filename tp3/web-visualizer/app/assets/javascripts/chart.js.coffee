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
            
            series = this.series
            pie = this.series[this.series.length-1]
            setInterval (->
              $.ajax(
                url: "tweets"
              ).done (data) ->
                x = (new Date()).getTime() # current time
                $.each(data.charts, (index, value) ->
                  i = window.parties.indexOf(value.name.toLowerCase())
                  y = value.quantity
                  series[i].addPoint [x, y], true, false
                  pie.data[i].y = y
                  pie.isDirty = true
                  pie.redraw()
                )
            ), 5000

      title:
        text: "Cantidad de tweets por partido político"

      xAxis:
        type: "datetime"
        tickPixelInterval: 150

      yAxis:
        title:
          text: "Cantidad"

        plotLines: [
          value: 0
          width: 1
          color: "#808080"
        ]

      tooltip:
        formatter: ->
          "<b>" + @series.name + "</b><br/>" + Highcharts.dateFormat("%Y-%m-%d %H:%M:%S", @x) + "<br/>" + Highcharts.numberFormat(@y, 2)

      legend:
        enabled: true

      exporting:
        enabled: true

      series: [
        { name: 'pro', data: []},
        { name: 'unen', data: []},
        { name: 'otros', data: []},
        { name: 'frente renovador', data: []},
        { name: 'fpv', data: []},
        { 
          type: 'pie',
          name: 'Total consumption',
          data: [{
            name: 'Pro',
            y: 0,
            color: Highcharts.getOptions().colors[0] # Jane's color
          }, {
            name: 'Unen',
            y: 0,
            color: Highcharts.getOptions().colors[1] # John's color
          }, {
            name: 'Otros',
            y: 0,
            color: Highcharts.getOptions().colors[2] # Joe's color
          }, {
            name: 'Frente Renovador',
            y: 0,
            color: Highcharts.getOptions().colors[3] # Joe's color
          }, {
            name: 'FPV',
            y: 0,
            color: Highcharts.getOptions().colors[4] # Joe's color
          }],
          center: [100, 80],
          size: 150,
          dataLabels: {
            enabled: false
          }
        }
      ]
