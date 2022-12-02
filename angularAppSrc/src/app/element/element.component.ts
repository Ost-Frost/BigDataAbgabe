import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: '[app-element]',
  templateUrl: './element.component.html',
  styleUrls: ['./element.component.css']
})
export class ElementComponent implements OnInit {

  @Input() name: string = "";
  @Input() id: string = "";
  @Input() imageUrl: string = "";

  constructor() { }

  ngOnInit(): void {

  }

}
